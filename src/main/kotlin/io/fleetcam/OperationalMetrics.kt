package io.fleetcam

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PreDestroy
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.common.TopicPartition
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

@Component
class OperationalMetrics(registry: MeterRegistry, db: JdbcTemplate) {
    init {
        fun register(name: String, query: String) {
            Gauge.builder(name, db) { jdbc ->
                try { (jdbc.queryForObject(query, Long::class.java) ?: 0L).toDouble() }
                catch (_: Exception) { Double.NaN }
            }.register(registry)
        }
        register("fleetcam.outbox.pending", "SELECT count(*) FROM outbox WHERE delivered_at IS NULL")
        register("fleetcam.events.unprocessed", "SELECT count(*) FROM ingest_events WHERE processed_at IS NULL")
        register("fleetcam.deadletters.open", "SELECT count(*) FROM dead_letters WHERE status='OPEN'")
        register("fleetcam.incidents.open", "SELECT count(*) FROM incidents WHERE status='OPEN'")
        register("fleetcam.evidence.pending", "SELECT count(*) FROM evidence WHERE status='PENDING'")
    }
}

/** Broker-side lag of the projector group: end offset minus committed offset, summed over partitions. */
@Component
class ConsumerLagMonitor(registry: MeterRegistry, private val admin: KafkaAdmin,
    @Value("\${spring.kafka.consumer.group-id}") private val group: String) {
    private val topic = "fleetcam.ingestion.v1"
    private val lag = AtomicLong(-1)
    private val clientHolder = lazy { AdminClient.create(admin.configurationProperties) }
    private val client: AdminClient by clientHolder
    init {
        // NaN while the broker cannot be queried; an unknown lag is not reported as zero.
        Gauge.builder("fleetcam.kafka.consumer.lag", lag) { v -> v.get().takeIf { it >= 0 }?.toDouble() ?: Double.NaN }.register(registry)
    }
    @Scheduled(fixedDelay = 5000)
    fun sample() {
        lag.set(try {
            val partitions = client.describeTopics(listOf(topic)).allTopicNames().get(5, TimeUnit.SECONDS).getValue(topic)
                .partitions().map { TopicPartition(topic, it.partition()) }
            val committed = client.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS)
            val end = client.listOffsets(partitions.associateWith { OffsetSpec.latest() }).all().get(5, TimeUnit.SECONDS)
            val start = client.listOffsets(partitions.associateWith { OffsetSpec.earliest() }).all().get(5, TimeUnit.SECONDS)
            partitions.sumOf { tp -> end.getValue(tp).offset() - (committed[tp]?.offset() ?: start.getValue(tp).offset()) }
        } catch (_: Exception) { -1L })
    }
    @PreDestroy fun close() { if (clientHolder.isInitialized()) runCatching { client.close() } }
}
