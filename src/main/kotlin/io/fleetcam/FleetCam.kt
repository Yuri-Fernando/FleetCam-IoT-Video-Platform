package io.fleetcam

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import io.micrometer.tracing.Tracer
import org.springframework.beans.factory.annotation.Value
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.apache.kafka.common.TopicPartition
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.bind.annotation.*
import org.springframework.web.filter.OncePerRequestFilter
import org.springframework.web.server.ResponseStatusException
import org.springframework.util.backoff.FixedBackOff
import java.security.MessageDigest
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

@SpringBootApplication
@EnableScheduling
class FleetCamApplication {
    @Bean fun eventsTopic() = TopicBuilder.name("fleetcam.ingestion.v1").partitions(3).replicas(1).build()
    @Bean fun dltTopic() = TopicBuilder.name("fleetcam.ingestion.dlt.v1").partitions(3).replicas(1).build()
    @Bean fun kafkaErrorHandler(kafka: KafkaTemplate<String,String>): DefaultErrorHandler {
        val recoverer = DeadLetterPublishingRecoverer(kafka) { record, _ -> TopicPartition("fleetcam.ingestion.dlt.v1",record.partition()) }
        recoverer.setFailIfSendResultIsError(true)
        return DefaultErrorHandler(recoverer, FixedBackOff(1000L, 2L))
    }
    @Bean fun dltListenerFactory(consumerFactory: ConsumerFactory<Any,Any>): ConcurrentKafkaListenerContainerFactory<Any,Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<Any,Any>()
        factory.consumerFactory = consumerFactory
        factory.setCommonErrorHandler(DefaultErrorHandler(FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS)))
        return factory
    }
}
fun main(args: Array<String>) { runApplication<FleetCamApplication>(*args) }

// Local lab credentials bind the request to a tenant; not a production identity provider.
object TenantContext {
    private val current = ThreadLocal<String>()
    fun set(tenant: String) { current.set(tenant) }
    fun clear() { current.remove() }
    fun requireTenant(): String = current.get() ?: throw IllegalStateException("No authenticated tenant")
}
private fun tenantId(): String = TenantContext.requireTenant()

@Component
class LabAuth(@Value("\${fleetcam.api-key}") private val key: String,
              @Value("\${fleetcam.tenant-b-api-key}") private val tenantBKey: String) : OncePerRequestFilter() {
    init { require(key.isNotBlank() && tenantBKey.isNotBlank() && key != tenantBKey) }
    override fun doFilterInternal(request: HttpServletRequest, response: HttpServletResponse, chain: FilterChain) {
        if (!request.requestURI.startsWith("/api/")) { chain.doFilter(request,response); return }
        val supplied = request.getHeader("X-API-Key") ?: ""
        fun matches(expected: String) = MessageDigest.isEqual(supplied.toByteArray(Charsets.UTF_8), expected.toByteArray(Charsets.UTF_8))
        val tenant = when { matches(key) -> "tenant-demo"; matches(tenantBKey) -> "tenant-b"; else -> null }
        if (tenant == null) { response.sendError(401); return }
        if (request.requestURI.startsWith("/api/v1/lab/") && tenant != "tenant-demo") { response.sendError(403); return }
        TenantContext.set(tenant)
        try { chain.doFilter(request,response) } finally { TenantContext.clear() }
    }
}

data class DeviceRequest(val id: String = "", val vehicle_id: String = "", val model: String = "", val firmware_version: String = "")
data class LifecycleRequest(val state: String = "")
private fun invalid(message: String): Nothing = throw ResponseStatusException(HttpStatus.BAD_REQUEST, message)
private fun required(node: JsonNode, field: String): String {
    val v = node.path(field)
    if (!v.isTextual || v.asText().isBlank() || v.asText().length > 200) invalid("Invalid $field")
    return v.asText()
}
private fun hash(bytes: ByteArray) = MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }

@Service
class Registry(private val db: JdbcTemplate, private val mapper: ObjectMapper, private val tracerProvider: ObjectProvider<Tracer>) {
    fun provision(d: DeviceRequest) {
        if (listOf(d.id, d.vehicle_id, d.model, d.firmware_version).any { it.isBlank() || it.length > 200 }) invalid("Invalid device")
        val n = db.update("INSERT INTO devices(tenant_id,id,vehicle_id,model,firmware_version) VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING", tenantId(),d.id,d.vehicle_id,d.model,d.firmware_version)
        if (n == 0) throw ResponseStatusException(HttpStatus.CONFLICT, "Device already exists")
    }

    @Transactional
    fun lifecycle(id: String, requested: LifecycleRequest) {
        if (requested.state !in listOf("ACTIVE", "MAINTENANCE", "DECOMMISSIONED")) invalid("Invalid lifecycle state")
        val prior = db.query("SELECT lifecycle_state FROM devices WHERE tenant_id=? AND id=? FOR UPDATE", { rs, _ -> rs.getString(1) },tenantId(),id).firstOrNull()
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND,"Device not found")
        if (prior == "DECOMMISSIONED" && requested.state != "DECOMMISSIONED") throw ResponseStatusException(HttpStatus.CONFLICT,"Decommissioned device cannot be reactivated")
        if (prior == requested.state) return
        db.update("UPDATE devices SET lifecycle_state=? WHERE tenant_id=? AND id=?",requested.state,tenantId(),id)
        db.update("INSERT INTO audit_log(tenant_id,action,resource_id,actor) VALUES (?,?,?,'local-operator')",tenantId(),"LIFECYCLE_"+requested.state,id)
        if (requested.state in listOf("MAINTENANCE", "DECOMMISSIONED")) {
            val incidents = db.query("SELECT id FROM incidents WHERE tenant_id=? AND device_id=? AND type='CONNECTIVITY' AND status='OPEN'", { rs, _ -> rs.getObject(1, UUID::class.java) },tenantId(),id)
            for (incident in incidents) {
                db.update("UPDATE incidents SET status='CLOSED',closed_at=now() WHERE id=?",incident)
                db.update("INSERT INTO incident_timeline(incident_id,kind,annotation) VALUES (?,'SUPPRESSED',?)",incident,"Ciclo de vida: ${requested.state}")
            }
        }
    }

    @Transactional
    fun ingest(n: JsonNode): Map<String, Any> {
        if (!n.isObject || !n.path("schema_version").isIntegralNumber || n.path("schema_version").asInt(-1) != 1 || !n.path("payload").isObject) invalid("Invalid schema or payload")
        if (required(n, "tenant_id") != tenantId()) throw ResponseStatusException(HttpStatus.FORBIDDEN, "Unknown tenant")
        val eventId = try { UUID.fromString(required(n,"event_id")) } catch (_: IllegalArgumentException) { invalid("Invalid event_id") }
        val device = required(n,"device_id"); val boot = required(n,"boot_id"); val kind = required(n,"event_kind")
        if (kind !in listOf("device_heartbeat", "safety_event", "technical_event")) invalid("Unknown event_kind")
        required(n,"source")
        if (!n.path("simulated").isBoolean || !n.path("sequence_number").isIntegralNumber || !n.path("sequence_number").canConvertToLong() || n.path("sequence_number").asLong() < 0) invalid("Invalid sequence_number or simulated")
        if (kind == "device_heartbeat") {
            val payload = n.path("payload")
            val sd = payload.path("sd")
            if (!sd.isMissingNode && !sd.isNull) {
                if (!sd.isObject) invalid("Invalid sd")
                val present = sd.path("present")
                if (!present.isMissingNode && !present.isNull && !present.isBoolean) invalid("Invalid sd.present")
                for (field in listOf("total_bytes", "free_bytes", "write_error_count")) {
                    val value = sd.path(field)
                    if (!value.isMissingNode && !value.isNull && (!value.isIntegralNumber || !value.canConvertToLong() || value.asLong() < 0)) invalid("Invalid sd.$field")
                }
                if (sd.path("total_bytes").isIntegralNumber && sd.path("free_bytes").isIntegralNumber && sd.path("free_bytes").asLong() > sd.path("total_bytes").asLong()) invalid("SD free space exceeds capacity")
            }
            val camera = payload.path("camera")
            if (!camera.isMissingNode && !camera.isNull) {
                if (!camera.isObject) invalid("Invalid camera")
                val capture = camera.path("capture_operational")
                if (!capture.isMissingNode && !capture.isNull && !capture.isBoolean) invalid("Invalid capture_operational")
            }
        }
        val seq = n.path("sequence_number").asLong()
        val occurred = try { Instant.parse(required(n,"occurred_at")) } catch (_: Exception) { invalid("Invalid occurred_at") }
        val lifecycle = db.query("SELECT lifecycle_state FROM devices WHERE tenant_id=? AND id=? FOR UPDATE", { rs, _ -> rs.getString(1) },tenantId(),device).firstOrNull()
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND,"Device not found")
        if (lifecycle == "DECOMMISSIONED") throw ResponseStatusException(HttpStatus.CONFLICT,"Device decommissioned")
        // Canonical JSON means field order does not make a replay a conflict.
        val canonical = mapper.writer().with(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS).writeValueAsString(mapper.convertValue(n, java.util.TreeMap::class.java))
        val digest = hash(canonical.toByteArray(Charsets.UTF_8))
        val inserted = db.update("""INSERT INTO ingest_events(tenant_id,event_id,device_id,boot_id,sequence_number,event_kind,payload_hash,payload,occurred_at)
            VALUES (?,?,?,?,?,?,?,?::jsonb,?) ON CONFLICT DO NOTHING""",tenantId(),eventId,device,boot,seq,kind,digest,canonical,java.sql.Timestamp.from(occurred))
        if (inserted == 0) {
            val old = db.query("SELECT payload_hash FROM ingest_events WHERE tenant_id=? AND event_id=?", { rs, _ -> rs.getString(1) },tenantId(),eventId).firstOrNull()
            if (old != digest) throw ResponseStatusException(HttpStatus.CONFLICT,"Event identity or sequence conflict")
            return mapOf("event_id" to eventId.toString(), "status" to "duplicate")
        }
        val span = tracerProvider.ifAvailable?.currentSpan()?.context()
        val traceparent = span?.let { "00-${it.traceId()}-${it.spanId()}-${if (it.sampled() == true) "01" else "00"}" }
        db.update("INSERT INTO outbox(tenant_id,event_id,device_id,payload,traceparent) VALUES (?,?,?,?,?)",tenantId(),eventId,device,canonical,traceparent)
        if (kind == "device_heartbeat" && lifecycle == "PROVISIONED") db.update("UPDATE devices SET lifecycle_state='ACTIVE' WHERE tenant_id=? AND id=?",tenantId(),device)
        return mapOf("event_id" to eventId.toString(), "status" to "accepted")
    }
}

@Service
class Projector(private val db: JdbcTemplate, private val mapper: ObjectMapper, registry: io.micrometer.core.instrument.MeterRegistry,
    @Value("\${fleetcam.storage.low-enter:0.10}") lowEnter: Double,
    @Value("\${fleetcam.storage.low-exit:0.12}") lowExit: Double,
    @Value("\${fleetcam.storage.error-increases:2}") errorIncreases: Int,
    @Value("\${fleetcam.storage.recovery-observations:2}") recoveryObservations: Int) {
    private val limits = StorageThresholds(lowEnter, lowExit, errorIncreases, recoveryObservations)
    // Counts redeliveries recognized as already projected (at-least-once made visible).
    private val redeliveries = registry.counter("fleetcam.projector.redeliveries")
    @Transactional
    fun process(body: String) {
        val n = mapper.readTree(body)
        val tenant = required(n,"tenant_id"); val id = UUID.fromString(required(n,"event_id")); val device = required(n,"device_id")
        val marked = db.update("UPDATE ingest_events SET processed_at=now() WHERE tenant_id=? AND event_id=? AND device_id=? AND payload=?::jsonb AND processed_at IS NULL",tenant,id,device,body)
        if (marked == 0) {
            val duplicate = db.queryForObject("SELECT count(*) FROM ingest_events WHERE tenant_id=? AND event_id=? AND device_id=? AND payload=?::jsonb AND processed_at IS NOT NULL",Long::class.java,tenant,id,device,body)
            if (duplicate == 1L) { redeliveries.increment(); return }
            throw IllegalArgumentException("Kafka message does not match an accepted event")
        }
        if (n.path("event_kind").asText() != "device_heartbeat") return
        // Serialize with lifecycle changes: accepted historical events must not revive a retired device.
        val lifecycle = db.queryForObject("SELECT lifecycle_state FROM devices WHERE tenant_id=? AND id=? FOR UPDATE", String::class.java, tenant, device)
        if (lifecycle == "DECOMMISSIONED") return
        val p = n.path("payload"); val sd = p.path("sd"); val camera = p.path("camera")
        val capture = if (!camera.path("capture_operational").isBoolean) "UNKNOWN" else if (camera.path("capture_operational").asBoolean()) "HEALTHY" else "NO_FRAMES"
        // Same-boot sequence wins. Across boots, device time is a provisional ordering rule.
        val previous = db.queryForList("SELECT storage_state,capture_state,boot_id,sd_last_errors,sd_error_streak,sd_recovery_streak FROM device_state WHERE tenant_id=? AND device_id=?",tenant,device).firstOrNull()
        val priorStorage = previous?.takeIf { it["boot_id"] == required(n,"boot_id") }?.let {
            StorageObservation(it["storage_state"] as String, (it["sd_last_errors"] as? Number)?.toLong(), (it["sd_error_streak"] as Number).toInt(), (it["sd_recovery_streak"] as Number).toInt())
        }
        val observation = storageObservation(sd, priorStorage, limits)
        val storage = observation.state
        val currentBoot = required(n,"boot_id")
        if (previous != null && previous["boot_id"] != currentBoot && db.queryForObject(
            "SELECT count(*) FROM device_boot_sessions WHERE tenant_id=? AND device_id=? AND boot_id=? AND retired_at IS NOT NULL", Long::class.java,tenant,device,currentBoot) != 0L) return
        val changed = db.update("""INSERT INTO device_state(tenant_id,device_id,boot_id,sequence_number,occurred_at,storage_state,capture_state,firmware_version,sd_last_errors,sd_error_streak,sd_recovery_streak)
          VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (tenant_id,device_id) DO UPDATE SET
          boot_id=EXCLUDED.boot_id,sequence_number=EXCLUDED.sequence_number,occurred_at=EXCLUDED.occurred_at,
          storage_state=EXCLUDED.storage_state,capture_state=EXCLUDED.capture_state,firmware_version=EXCLUDED.firmware_version,sd_last_errors=EXCLUDED.sd_last_errors,sd_error_streak=EXCLUDED.sd_error_streak,sd_recovery_streak=EXCLUDED.sd_recovery_streak,updated_at=now()
          WHERE (device_state.boot_id=EXCLUDED.boot_id AND EXCLUDED.sequence_number>device_state.sequence_number)
             OR (device_state.boot_id<>EXCLUDED.boot_id AND EXCLUDED.occurred_at>device_state.occurred_at)""",
            tenant,device,required(n,"boot_id"),n.path("sequence_number").asLong(),java.sql.Timestamp.from(Instant.parse(required(n,"occurred_at"))),storage,capture,p.path("firmware_version").takeIf { it.isTextual }?.asText(),observation.lastErrors,observation.errorStreak,observation.recoveryStreak)
        if (changed == 0) return
        if (previous != null && previous["boot_id"] != currentBoot) {
            db.update("INSERT INTO device_boot_sessions(tenant_id,device_id,boot_id,retired_at) VALUES (?,?,?,now()) ON CONFLICT (tenant_id,device_id,boot_id) DO UPDATE SET retired_at=coalesce(device_boot_sessions.retired_at,now())",tenant,device,previous["boot_id"])
        }
        db.update("INSERT INTO device_boot_sessions(tenant_id,device_id,boot_id,firmware_version) VALUES (?,?,?,?) ON CONFLICT DO NOTHING",tenant,device,currentBoot,p.path("firmware_version").takeIf { it.isTextual }?.asText())
        if (previous?.get("storage_state") != storage || previous?.get("capture_state") != capture) {
            db.update("INSERT INTO state_transitions(tenant_id,device_id,old_storage,new_storage,old_capture,new_capture,event_id) VALUES (?,?,?,?,?,?,?)",
                tenant,device,previous?.get("storage_state"),storage,previous?.get("capture_state"),capture,id)
        }
        val badStorage = storage in listOf("NO_MEDIA", "WRITE_ERROR", "LOW_CAPACITY")
        updateIncident(tenant, device, "STORAGE", badStorage, storage != "UNKNOWN", "Estado de armazenamento informado: $storage", id)
        updateIncident(tenant, device, "CAPTURE", capture == "NO_FRAMES", capture != "UNKNOWN", "Estado de captura informado: $capture", id)
    }

    private fun updateIncident(tenant: String, device: String, type: String, bad: Boolean, known: Boolean, summary: String, eventId: UUID) {
        if (bad) {
            val incident = UUID.randomUUID()
            val inserted = db.update("INSERT INTO incidents(id,tenant_id,device_id,type,status,summary,source_event_id) VALUES (?,?,?,?,'OPEN',?,?) ON CONFLICT DO NOTHING",incident,tenant,device,type,summary,eventId)
            if (inserted == 1) db.update("INSERT INTO incident_timeline(incident_id,kind,annotation,event_id) VALUES (?,'OPENED',?,?)",incident,summary,eventId)
        } else if (known) {
            val open = db.query("SELECT id FROM incidents WHERE tenant_id=? AND device_id=? AND type=? AND status='OPEN'", { rs, _ -> rs.getObject(1, UUID::class.java) },tenant,device,type)
            for (incident in open) {
                db.update("UPDATE incidents SET status='CLOSED',closed_at=now() WHERE id=? AND status='OPEN'",incident)
                db.update("INSERT INTO incident_timeline(incident_id,kind,annotation,event_id) VALUES (?,'RECOVERED',?,?)",incident,summary,eventId)
            }
        }
    }
}

@Component
class Pipeline(private val db: JdbcTemplate, private val kafka: KafkaTemplate<String,String>, private val projector: Projector, private val mapper: ObjectMapper,
               private val tx: org.springframework.transaction.support.TransactionTemplate) {
    // A full batch means backlog: keep draining within a time budget; the budget keeps the shared scheduler responsive.
    @Scheduled(fixedDelay = 2000)
    fun publish() {
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1500)
        while (publishBatch() == 50 && System.nanoTime() < deadline) { /* drain */ }
    }

    /** Returns rows delivered, or -1 after a broker failure (which ends the drain). */
    private fun publishBatch(): Int {
        val rows = db.queryForList("SELECT tenant_id,event_id,device_id,payload,traceparent FROM outbox WHERE delivered_at IS NULL ORDER BY created_at LIMIT 50")
        val sent = ArrayList<Pair<Map<String,Any>, java.util.concurrent.CompletableFuture<*>>>()
        var failed = false
        fun fail(row: Map<String,Any>, e: Exception) {
            failed = true
            db.update("UPDATE outbox SET attempts=attempts+1,last_error=? WHERE tenant_id=? AND event_id=?",e.javaClass.simpleName,row["tenant_id"],row["event_id"])
        }
        for (row in rows) {
            val record = ProducerRecord("fleetcam.ingestion.v1", "${row["tenant_id"]}:${row["device_id"]}", row["payload"] as String)
            (row["traceparent"] as? String)?.let { record.headers().add("traceparent", it.toByteArray(Charsets.US_ASCII)) }
            try { sent += row to kafka.send(record) } catch (e: Exception) { fail(row, e); break }
        }
        val delivered = ArrayList<Array<Any?>>()
        for ((row, future) in sent) {
            try { future.get(12, TimeUnit.SECONDS); delivered += arrayOf(row["tenant_id"], row["event_id"]) }
            catch (e: Exception) { fail(row, e) }
        }
        // One commit per batch instead of per row: each commit costs an fsync (~55 ms on the lab volume, docs/benchmark.md).
        // A crash before this commit republishes the batch; the projector recognizes the redelivery.
        if (delivered.isNotEmpty()) tx.executeWithoutResult {
            db.batchUpdate("UPDATE outbox SET delivered_at=now(),attempts=attempts+1,last_error=NULL WHERE tenant_id=? AND event_id=?",delivered)
        }
        return if (failed) -1 else delivered.size
    }
    @KafkaListener(topics = ["fleetcam.ingestion.v1"])
    fun consume(record: ConsumerRecord<String,String>) = projector.process(record.value())
    @KafkaListener(topics = ["fleetcam.ingestion.dlt.v1"], containerFactory = "dltListenerFactory")
    fun deadLetter(record: ConsumerRecord<String,String>) {
        val error = record.headers().lastHeader("kafka_dlt-exception-message")?.value()?.toString(Charsets.UTF_8)?.take(1000)
        val owner = try {
            val message = mapper.readTree(record.value())
            val candidate = required(message,"tenant_id")
            val eventId = UUID.fromString(required(message,"event_id"))
            if (db.queryForObject("SELECT count(*) FROM ingest_events WHERE tenant_id=? AND event_id=? AND payload=?::jsonb",Long::class.java,candidate,eventId,record.value()) == 1L) candidate else null
        } catch (_: com.fasterxml.jackson.core.JsonProcessingException) { null }
          catch (_: IllegalArgumentException) { null }
          catch (_: ResponseStatusException) { null }
          catch (_: NullPointerException) { null }
        db.update("INSERT INTO dead_letters(id,topic,kafka_partition,kafka_offset,message_key,payload,error,tenant_id) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
            UUID.randomUUID(),record.topic(),record.partition(),record.offset(),record.key(),record.value(),error,owner)
    }
}

@Service
class Redrive(private val db: JdbcTemplate, private val mapper: ObjectMapper) {
    @Transactional
    fun run(id: UUID) {
        val row = db.queryForList("SELECT * FROM dead_letters WHERE id=? AND tenant_id=? FOR UPDATE",id,tenantId()).firstOrNull()
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND,"Dead letter not found")
        if (row["status"] != "OPEN") throw ResponseStatusException(HttpStatus.CONFLICT,"Already redriven")
        val n = try { mapper.readTree(row["payload"] as String) } catch (_: Exception) { invalid("Cannot redrive malformed record") }
        val event = try { UUID.fromString(required(n,"event_id")) } catch (_: Exception) { invalid("Cannot redrive without event_id") }
        if (required(n,"tenant_id") != tenantId()) throw ResponseStatusException(HttpStatus.FORBIDDEN,"Unknown tenant")
        if (db.update("UPDATE outbox SET delivered_at=NULL WHERE tenant_id=? AND event_id=? AND EXISTS (SELECT 1 FROM ingest_events WHERE tenant_id=? AND event_id=? AND processed_at IS NULL)",tenantId(),event,tenantId(),event) != 1)
            throw ResponseStatusException(HttpStatus.CONFLICT,"No unprocessed accepted event")
        db.update("UPDATE dead_letters SET status='REDRIVEN',redriven_at=now() WHERE id=?",id)
        db.update("INSERT INTO audit_log(tenant_id,action,resource_id,actor) VALUES (?,'DLT_REDRIVE',?,'local-admin')",tenantId(),id.toString())
    }
}

@RestController
@RequestMapping("/api/v1/lab/dlt")
class DeadLetterApi(private val db: JdbcTemplate, private val redrive: Redrive) {
    @GetMapping fun list() = db.queryForList("SELECT id,topic,kafka_partition,kafka_offset,error,status,created_at,redriven_at FROM dead_letters WHERE tenant_id=? OR tenant_id IS NULL ORDER BY created_at DESC LIMIT 500",tenantId())
    @PostMapping("/{id}/redrive") fun redrive(@PathVariable id: UUID): Map<String,String> { redrive.run(id); return mapOf("status" to "queued") }
}

@RestController
@RequestMapping("/api/v1")
class Api(private val registry: Registry, private val db: JdbcTemplate,
          @Value("\${fleetcam.suspect-seconds}") private val suspect: Long,
          @Value("\${fleetcam.offline-seconds}") private val offline: Long) {
    @PostMapping("/devices") fun create(@RequestBody d: DeviceRequest): ResponseEntity<Any> {
        registry.provision(d); return ResponseEntity.status(201).body(mapOf("id" to d.id))
    }
    @PostMapping("/ingest/heartbeat", "/ingest/events") fun ingest(@RequestBody n: JsonNode): ResponseEntity<Any> = ResponseEntity.status(202).body(registry.ingest(n))
    @GetMapping("/devices") fun devices(): List<Map<String,Any>> = deviceRows(null)
    private fun deviceRows(id: String?): List<Map<String,Any>> = db.queryForList("""SELECT d.id,d.vehicle_id,d.model,d.firmware_version,d.lifecycle_state,s.storage_state,s.capture_state,s.boot_id,s.sequence_number,
       (SELECT max(i.received_at) FROM ingest_events i WHERE i.tenant_id=d.tenant_id AND i.device_id=d.id AND i.event_kind='device_heartbeat') AS last_seen_at,
       (SELECT count(*) FROM ingest_events i WHERE i.tenant_id=d.tenant_id AND i.device_id=d.id AND i.event_kind='device_heartbeat' AND i.received_at >= now() - (? * interval '1 second')) AS recent_heartbeats,
       EXISTS (SELECT 1 FROM incidents x WHERE x.tenant_id=d.tenant_id AND x.device_id=d.id AND x.type='CONNECTIVITY' AND x.status='OPEN') AS connectivity_incident_open
       FROM devices d LEFT JOIN device_state s ON s.tenant_id=d.tenant_id AND s.device_id=d.id WHERE d.tenant_id=? AND (?::text IS NULL OR d.id=?) ORDER BY d.id LIMIT 500""",suspect,tenantId(),id,id).map { row ->
        val last = (row["last_seen_at"] as? java.sql.Timestamp)?.toInstant()
        val age = last?.let { java.time.Duration.between(it, Instant.now()).seconds }
        row + mapOf("connectivity_state" to when { row["lifecycle_state"] == "DECOMMISSIONED" -> "DECOMMISSIONED"; row["lifecycle_state"] == "MAINTENANCE" -> "MAINTENANCE"; age == null -> "UNKNOWN"; age >= offline -> "OFFLINE"; age >= suspect -> "SUSPECT"; (row["recent_heartbeats"] as Number).toLong() < 2 || row["connectivity_incident_open"] == true -> "RECOVERING"; else -> "ONLINE" }, "storage_state" to (row["storage_state"] ?: "UNKNOWN"), "capture_state" to (row["capture_state"] ?: "UNKNOWN"))
    }
    @PostMapping("/devices/{id}/lifecycle") fun lifecycle(@PathVariable id: String, @RequestBody r: LifecycleRequest): Map<String,String> { registry.lifecycle(id,r); return mapOf("status" to r.state) }
    @GetMapping("/devices/{id}/timeline") fun timeline(@PathVariable id: String): List<Map<String,Any>> = db.queryForList("SELECT i.event_id,i.event_kind,i.occurred_at,i.received_at,i.processed_at,i.payload::text AS payload,o.traceparent FROM ingest_events i LEFT JOIN outbox o ON o.tenant_id=i.tenant_id AND o.event_id=i.event_id WHERE i.tenant_id=? AND i.device_id=? ORDER BY i.received_at DESC LIMIT 500",tenantId(),id)
    @GetMapping("/devices/{id}") fun device(@PathVariable id: String): Map<String,Any> = deviceRows(id).firstOrNull()
        ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "Device not found")
    @GetMapping("/events") fun events(): List<Map<String,Any>> = db.queryForList("SELECT event_id,device_id,event_kind,occurred_at,received_at,processed_at,payload::text AS payload FROM ingest_events WHERE tenant_id=? AND event_kind<>'device_heartbeat' ORDER BY received_at DESC LIMIT 500",tenantId())
    @GetMapping("/incidents") fun incidents(): List<Map<String,Any>> = db.queryForList("SELECT id,device_id,type,status,opened_at,closed_at,summary,source_event_id FROM incidents WHERE tenant_id=? ORDER BY opened_at DESC LIMIT 500",tenantId())
    @GetMapping("/incidents/{id}") fun incident(@PathVariable id: UUID): Map<String,Any> {
        val incident = db.queryForList("SELECT id,device_id,type,status,opened_at,closed_at,summary,source_event_id FROM incidents WHERE tenant_id=? AND id=?",tenantId(),id).firstOrNull()
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND,"Incident not found")
        val timeline = db.queryForList("SELECT occurred_at,kind,annotation,event_id FROM incident_timeline WHERE incident_id=? ORDER BY occurred_at",id)
        return incident + mapOf("timeline" to timeline)
    }
    @GetMapping("/ops/overview") fun overview(): Map<String,Any> = mapOf("devices" to devices(), "outbox_pending" to (db.queryForObject("SELECT count(*) FROM outbox WHERE tenant_id=? AND delivered_at IS NULL",Long::class.java,tenantId()) ?: 0), "unprocessed" to (db.queryForObject("SELECT count(*) FROM ingest_events WHERE tenant_id=? AND processed_at IS NULL",Long::class.java,tenantId()) ?: 0))
}
