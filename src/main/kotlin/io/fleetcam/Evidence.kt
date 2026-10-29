package io.fleetcam

import io.minio.BucketExistsArgs
import io.minio.GetObjectArgs
import io.minio.GetPresignedObjectUrlArgs
import io.minio.MakeBucketArgs
import io.minio.PutObjectArgs
import io.minio.MinioClient
import io.minio.StatObjectArgs
import io.minio.RemoveObjectArgs
import io.minio.http.Method
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException
import java.security.MessageDigest
import java.io.ByteArrayInputStream
import java.util.UUID
import java.util.concurrent.TimeUnit

data class UploadRequest(val event_id: UUID, val mime_type: String, val byte_size: Long, val sha256: String, val origin: String, val idempotency_key: UUID? = null)

@Service
class EvidenceService(private val db: JdbcTemplate,
    @Value("\${FLEETCAM_S3_ENDPOINT:http://localhost:9000}") endpoint: String,
    @Value("\${FLEETCAM_S3_PUBLIC_ENDPOINT:http://localhost:9000}") publicEndpoint: String,
    @Value("\${FLEETCAM_S3_ACCESS_KEY:fleetcam_local}") accessKey: String,
    @Value("\${FLEETCAM_S3_SECRET_KEY:fleetcam_local_password}") secretKey: String,
    @Value("\${fleetcam.evidence.pending-timeout-seconds:900}") private val pendingTimeout: Long) {
    private val internal = MinioClient.builder().endpoint(endpoint).credentials(accessKey,secretKey).region("us-east-1").build()
    private val publicClient = MinioClient.builder().endpoint(publicEndpoint).credentials(accessKey,secretKey).region("us-east-1").build()
    private val bucket = "fleetcam-evidence"
    private val tenant: String get() = TenantContext.requireTenant()
    private fun ensureBucket() {
        if (!internal.bucketExists(BucketExistsArgs.builder().bucket(bucket).build())) {
            internal.makeBucket(MakeBucketArgs.builder().bucket(bucket).build())
        }
    }
    private fun signed(key: String, method: Method): String = publicClient.getPresignedObjectUrl(
        GetPresignedObjectUrlArgs.builder().bucket(bucket).`object`(key).method(method).expiry(5, TimeUnit.MINUTES).build())

    fun upload(r: UploadRequest): Map<String,Any> {
        if (r.mime_type !in listOf("image/jpeg", "image/png") || r.byte_size !in 1..10485760 ||
            !r.sha256.matches(Regex("[0-9a-f]{64}")) || r.origin !in listOf("SYNTHETIC", "VISIONBRAIN_ANALYSIS"))
            throw ResponseStatusException(HttpStatus.BAD_REQUEST,"Invalid evidence metadata")
        if (db.queryForObject("SELECT count(*) FROM ingest_events WHERE tenant_id=? AND event_id=?",Long::class.java,tenant,r.event_id) == 0L)
            throw ResponseStatusException(HttpStatus.NOT_FOUND,"Event not found")
        val id = r.idempotency_key?.let { UUID.nameUUIDFromBytes("fleetcam-evidence:$tenant:$it".toByteArray(Charsets.UTF_8)) } ?: UUID.randomUUID()
        val key = "$tenant/staging/$id"
        try { ensureBucket() } catch (e: Exception) { throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,"Object storage unavailable",e) }
        db.update("INSERT INTO evidence(id,tenant_id,event_id,object_key,mime_type,byte_size,sha256,origin) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT (id) DO NOTHING",id,tenant,r.event_id,key,r.mime_type,r.byte_size,r.sha256,r.origin)
        val existing = row(id)
        if (existing["event_id"] != r.event_id || existing["mime_type"] != r.mime_type || (existing["byte_size"] as Number).toLong() != r.byte_size || existing["sha256"] != r.sha256 || existing["origin"] != r.origin)
            throw ResponseStatusException(HttpStatus.CONFLICT,"Evidence identity conflict")
        if (existing["status"] == "AVAILABLE") return mapOf("id" to id,"status" to "AVAILABLE")
        if (existing["status"] == "EXPIRED") throw ResponseStatusException(HttpStatus.CONFLICT,"Evidence expired")
        // Every issued ticket restarts the reconciliation window; a retried abandoned upload is pending again.
        db.update("UPDATE evidence SET status='PENDING',failure_reason=NULL,upload_requested_at=now() WHERE id=? AND tenant_id=? AND status IN ('PENDING','FAILED')",id,tenant)
        return mapOf("id" to id, "status" to "PENDING", "upload_url" to signed(key, Method.PUT), "expires_in_seconds" to 300)
    }

    @Transactional(noRollbackFor = [ResponseStatusException::class])
    fun confirm(id: UUID): Map<String,Any> {
        val row = db.queryForList("SELECT * FROM evidence WHERE id=? AND tenant_id=? FOR UPDATE",id,tenant).firstOrNull()
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND,"Evidence not found")
        if (row["status"] == "AVAILABLE") return mapOf("id" to id, "status" to "AVAILABLE")
        if ((row["failure_reason"] as? String)?.startsWith("UPLOAD_") == true)
            throw ResponseStatusException(HttpStatus.CONFLICT,"Upload abandoned; request a new upload")
        val staging = row["object_key"] as String
        val key = "$tenant/verified/$id"
        val bytes: ByteArray = try {
            val stat = internal.statObject(StatObjectArgs.builder().bucket(bucket).`object`(staging).build())
            if (stat.size() != (row["byte_size"] as Number).toLong() || stat.size() > 10485760) {
                db.update("UPDATE evidence SET status='FAILED' WHERE id=? AND tenant_id=?",id,tenant)
                throw ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,"Evidence size mismatch")
            }
            internal.getObject(GetObjectArgs.builder().bucket(bucket).`object`(staging).build()).use { it.readNBytes(10485761) }
        } catch (e: ResponseStatusException) { throw e }
          catch (e: Exception) { throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,"Evidence unavailable; still pending",e) }
        val digest = MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
        val png = bytes.size >= 8 && bytes.take(8).map { it.toInt() and 255 } == listOf(137,80,78,71,13,10,26,10)
        val jpeg = bytes.size >= 3 && (bytes[0].toInt() and 255) == 255 && (bytes[1].toInt() and 255) == 216 && (bytes[2].toInt() and 255) == 255
        val valid = bytes.size.toLong() == (row["byte_size"] as Number).toLong() && digest == row["sha256"] && ((row["mime_type"] == "image/png" && png) || (row["mime_type"] == "image/jpeg" && jpeg))
        if (!valid) {
            db.update("UPDATE evidence SET status='FAILED' WHERE id=? AND tenant_id=?",id,tenant)
            throw ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,"Evidence digest or signature mismatch")
        }
        try {
            internal.putObject(PutObjectArgs.builder().bucket(bucket).`object`(key)
                .stream(ByteArrayInputStream(bytes), bytes.size.toLong(), -1)
                .contentType(row["mime_type"] as String).build())
        } catch (e: Exception) {
            throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,"Could not store verified evidence",e)
        }
        db.update("UPDATE evidence SET status='AVAILABLE',object_key=?,confirmed_at=now() WHERE id=? AND tenant_id=?",key,id,tenant)
        return mapOf("id" to id,"status" to "AVAILABLE")
    }
    fun access(id: UUID): Map<String,Any> {
        val row = row(id)
        if (row["status"] != "AVAILABLE" || db.queryForObject("SELECT retention_until > now() FROM evidence WHERE id=? AND tenant_id=?",Boolean::class.java,id,tenant) != true)
            throw ResponseStatusException(HttpStatus.CONFLICT,"Evidence not available or expired")
        return mapOf("url" to signed(row["object_key"] as String, Method.GET), "expires_in_seconds" to 300)
    }
    private fun row(id: UUID) = db.queryForList("SELECT * FROM evidence WHERE id=? AND tenant_id=?",id,tenant).firstOrNull()
        ?: throw ResponseStatusException(HttpStatus.NOT_FOUND,"Evidence not found")
    fun list() = db.queryForList("SELECT id,event_id,mime_type,byte_size,sha256,origin,status,failure_reason,created_at,confirmed_at,retention_until FROM evidence WHERE tenant_id=? ORDER BY created_at DESC LIMIT 500",tenant)

    @Scheduled(fixedDelay = 60000)
    fun reconcileScheduled() { reconcile(pendingTimeout, null) }

    /** Interrupted uploads never confirmed within the window become FAILED; the event keeps no available evidence. */
    fun reconcile(olderThanSeconds: Long, onlyTenant: String?): Map<String,Int> {
        val stale = db.queryForList("SELECT id,tenant_id,object_key,byte_size FROM evidence WHERE status='PENDING' AND upload_requested_at <= now() - (? * interval '1 second') AND (?::text IS NULL OR tenant_id=?) ORDER BY upload_requested_at LIMIT 100",olderThanSeconds,onlyTenant,onlyTenant)
        var missing = 0; var unconfirmed = 0
        for (item in stale) {
            val id = item["id"] as UUID
            val staging = item["object_key"] as String
            val present = try { internal.statObject(StatObjectArgs.builder().bucket(bucket).`object`(staging).build()); true }
                catch (e: io.minio.errors.ErrorResponseException) { if (e.errorResponse().code() == "NoSuchKey") false else continue }
                catch (_: Exception) { continue } // Storage unavailable: leave PENDING and retry later.
            val reason = if (present) "UPLOAD_UNCONFIRMED" else "UPLOAD_MISSING"
            // The status guard serializes with confirm(), which holds the row lock while verifying.
            if (db.update("UPDATE evidence SET status='FAILED',failure_reason=? WHERE id=? AND status='PENDING'",reason,id) == 1) {
                if (present) try { internal.removeObject(RemoveObjectArgs.builder().bucket(bucket).`object`(staging).build()) } catch (_: Exception) { /* expire() retries removal. */ }
                if (present) unconfirmed++ else missing++
            }
        }
        return mapOf("upload_missing" to missing, "upload_unconfirmed" to unconfirmed)
    }
    @Scheduled(fixedDelay = 3600000)
    fun expire() {
        val expired = db.queryForList("SELECT id,tenant_id FROM evidence WHERE retention_until<=now() AND status<>'EXPIRED' LIMIT 100")
        for (item in expired) {
            val id = item["id"] as UUID
            val tenant = item["tenant_id"] as String
            try {
                for (key in listOf("$tenant/staging/$id", "$tenant/verified/$id"))
                    internal.removeObject(RemoveObjectArgs.builder().bucket(bucket).`object`(key).build())
                db.update("UPDATE evidence SET status='EXPIRED' WHERE id=? AND tenant_id=?",id,tenant)
            } catch (_: Exception) { /* Retain the row for retry; never claim successful deletion. */ }
        }
    }
}

@RestController
@RequestMapping("/api/v1/evidence")
class EvidenceApi(private val service: EvidenceService) {
    @PostMapping("/uploads") fun upload(@RequestBody r: UploadRequest) = service.upload(r)
    @PostMapping("/{id}/confirm") fun confirm(@PathVariable id: UUID) = service.confirm(id)
    @GetMapping("/{id}/access") fun access(@PathVariable id: UUID) = service.access(id)
    @GetMapping fun list() = service.list()
}

@RestController
@RequestMapping("/api/v1/lab/evidence")
class EvidenceLabApi(private val service: EvidenceService) {
    @PostMapping("/reconcile") fun reconcile(@RequestParam("older_than_seconds") olderThan: Long): Map<String,Int> {
        if (olderThan !in 0..86400) throw ResponseStatusException(HttpStatus.BAD_REQUEST,"older_than_seconds must be 0..86400")
        return service.reconcile(olderThan, TenantContext.requireTenant())
    }
}
