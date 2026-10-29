package io.fleetcam

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.time.Instant
import java.util.UUID

@Testcontainers
@SpringBootTest(properties = ["spring.kafka.listener.auto-startup=false"])
class RedeliveryTest {
    companion object {
        @Container @JvmField val postgres = PostgreSQLContainer<Nothing>("postgres:16")
        @Container @JvmField val kafka = KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"))
        @JvmStatic @DynamicPropertySource fun properties(r: DynamicPropertyRegistry) {
            r.add("spring.datasource.url", postgres::getJdbcUrl)
            r.add("spring.datasource.username", postgres::getUsername)
            r.add("spring.datasource.password", postgres::getPassword)
            r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers)
        }
    }
    @Autowired lateinit var registry: Registry
    @Autowired lateinit var projector: Projector
    @Autowired lateinit var db: JdbcTemplate
    @Autowired lateinit var mapper: ObjectMapper

    private fun consumer(group: String): KafkaConsumer<String,String> = KafkaConsumer(mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
        ConsumerConfig.GROUP_ID_CONFIG to group,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
    ))
    private fun read(c: KafkaConsumer<String,String>, eventId: UUID): String {
        val deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos()
        while (System.nanoTime() < deadline) {
            for (record in c.poll(Duration.ofMillis(500))) {
                if (mapper.readTree(record.value()).path("event_id").asText() == eventId.toString()) return record.value()
            }
        }
        throw AssertionError("event not delivered")
    }

    @Test fun `uncommitted delivery is redelivered without duplicate effect`() {
        val id = UUID.randomUUID(); val device = "it-" + UUID.randomUUID().toString()
        TenantContext.set("tenant-demo")
        try { registry.provision(DeviceRequest(device, "vehicle", "virtual", "sim-1")) } finally { TenantContext.clear() }
        val body = mapper.readTree("""{"schema_version":1,"event_id":"$id","tenant_id":"tenant-demo","device_id":"$device","boot_id":"boot","sequence_number":1,"event_kind":"device_heartbeat","source":"TEST","simulated":true,"occurred_at":"${Instant.now()}","payload":{"sd":{"present":true,"total_bytes":1000,"free_bytes":800,"write_error_count":0}}}""")
        TenantContext.set("tenant-demo")
        try { registry.ingest(body) } finally { TenantContext.clear() }
        val group = "test-redelivery-" + UUID.randomUUID()
        val first = consumer(group)
        first.subscribe(listOf("fleetcam.ingestion.v1"))
        val record = try { read(first, id).also { projector.process(it) } } finally { first.close(Duration.ZERO) }
        val processedAt = db.queryForObject("SELECT processed_at FROM ingest_events WHERE tenant_id=? AND event_id=?", java.sql.Timestamp::class.java, "tenant-demo", id)
        assertNotNull(processedAt)
        val second = consumer(group)
        try {
            second.subscribe(listOf("fleetcam.ingestion.v1"))
            val redelivered = read(second, id)
            assertEquals(record, redelivered)
            projector.process(redelivered)
            second.commitSync()
        } finally { second.close() }
        assertEquals(processedAt, db.queryForObject("SELECT processed_at FROM ingest_events WHERE tenant_id=? AND event_id=?", java.sql.Timestamp::class.java, "tenant-demo", id))
        assertEquals(1L, db.queryForObject("SELECT count(*) FROM device_state WHERE tenant_id=? AND device_id=?", Long::class.java, "tenant-demo", device))
        assertEquals(1L, db.queryForObject("SELECT count(*) FROM state_transitions WHERE tenant_id=? AND device_id=?", Long::class.java, "tenant-demo", device))
    }
}


