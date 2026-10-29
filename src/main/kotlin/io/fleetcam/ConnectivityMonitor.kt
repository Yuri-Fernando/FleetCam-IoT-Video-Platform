package io.fleetcam

import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

@Service
class ConnectivityMonitor(private val db: JdbcTemplate,
    @Value("\${fleetcam.offline-seconds}") private val offlineSeconds: Long) {
    @Scheduled(fixedDelay = 10000)
    @Transactional
    fun reconcile() {
        // Accepted heartbeats are used directly so consumer lag does not imply offline.
        val devices = db.queryForList("""SELECT d.tenant_id,d.id,
            max(i.received_at) AS last_seen,
            max(i.received_at) < now() - (? * interval '1 second') AS offline
            FROM devices d LEFT JOIN ingest_events i ON i.tenant_id=d.tenant_id AND i.device_id=d.id AND i.event_kind='device_heartbeat'
            WHERE d.lifecycle_state NOT IN ('MAINTENANCE','DECOMMISSIONED')
            GROUP BY d.tenant_id,d.id""",offlineSeconds)
        for (device in devices) {
            if (device["last_seen"] == null) continue
            val tenant = device["tenant_id"] as String; val id = device["id"] as String
            val lifecycle = db.query("SELECT lifecycle_state FROM devices WHERE tenant_id=? AND id=? FOR UPDATE", { rs, _ -> rs.getString(1) },tenant,id).firstOrNull()
            if (lifecycle == null || lifecycle in listOf("MAINTENANCE", "DECOMMISSIONED")) continue
            if (device["offline"] == true) {
                val incident = UUID.randomUUID(); val summary = "Heartbeat não recebido dentro da janela configurada."
                val inserted = db.update("INSERT INTO incidents(id,tenant_id,device_id,type,status,summary) VALUES (?,?,?,'CONNECTIVITY','OPEN',?) ON CONFLICT DO NOTHING",incident,tenant,id,summary)
                if (inserted == 1) db.update("INSERT INTO incident_timeline(incident_id,kind,annotation) VALUES (?,'OPENED',?)",incident,summary)
            } else {
                val open = db.query("""SELECT inc.id FROM incidents inc WHERE inc.tenant_id=? AND inc.device_id=? AND inc.type='CONNECTIVITY' AND inc.status='OPEN' AND (SELECT count(*) FROM ingest_events e WHERE e.tenant_id=inc.tenant_id AND e.device_id=inc.device_id AND e.event_kind='device_heartbeat' AND e.received_at > inc.opened_at) >= 2""", { rs, _ -> rs.getObject(1, UUID::class.java) },tenant,id)
                for (incident in open) {
                    db.update("UPDATE incidents SET status='CLOSED',closed_at=now() WHERE id=? AND status='OPEN'",incident)
                    db.update("INSERT INTO incident_timeline(incident_id,kind,annotation) VALUES (?,'RECOVERED','Dois heartbeats aceitos após o incidente.')",incident)
                }
            }
        }
    }
}
