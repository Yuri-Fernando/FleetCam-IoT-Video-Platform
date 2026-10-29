package io.fleetcam

import com.fasterxml.jackson.databind.JsonNode

data class StorageObservation(val state: String, val lastErrors: Long?, val errorStreak: Int, val recoveryStreak: Int)

data class StorageThresholds(val lowEnter: Double = 0.10, val lowExit: Double = 0.12, val errorIncreases: Int = 2, val recoveryObservations: Int = 2) {
    init { require(lowEnter in 0.0..1.0 && lowExit in lowEnter..1.0 && errorIncreases > 0 && recoveryObservations > 0) }
}

/** Repeated increases raise an I/O fault; stable observations clear it. */
fun storageObservation(sd: JsonNode, previous: StorageObservation?, limits: StorageThresholds = StorageThresholds()): StorageObservation {
    if (!sd.isObject || !sd.path("present").isBoolean) return StorageObservation("UNKNOWN", null, 0, 0)
    if (!sd.path("present").asBoolean()) return StorageObservation("NO_MEDIA", null, 0, 0)
    val errors = sd.path("write_error_count").takeIf { it.isIntegralNumber && it.canConvertToLong() }?.asLong()
    if (errors == null) return StorageObservation("UNKNOWN", null, 0, 0)
    val prior = previous?.lastErrors
    val increase = errors != null && prior != null && errors > prior
    val streak = if (increase) (previous?.errorStreak ?: 0) + 1 else 0
    val stable = errors != null && prior != null && errors == prior
    val recovery = if (stable && previous?.state == "WRITE_ERROR") previous.recoveryStreak + 1 else 0
    if (streak >= limits.errorIncreases || (previous?.state == "WRITE_ERROR" && recovery < limits.recoveryObservations))
        return StorageObservation("WRITE_ERROR", errors, streak, recovery)
    val total = sd.path("total_bytes").takeIf { it.isIntegralNumber && it.canConvertToLong() }?.asLong()
    val free = sd.path("free_bytes").takeIf { it.isIntegralNumber && it.canConvertToLong() }?.asLong()
    if (total != null && total > 0 && free != null) {
        val ratio = free.toDouble() / total
        if (ratio < limits.lowEnter || (previous?.state == "LOW_CAPACITY" && ratio < limits.lowExit))
            return StorageObservation("LOW_CAPACITY", errors, streak, recovery)
        if (prior != null || errors == 0L) return StorageObservation("HEALTHY", errors, streak, recovery)
    }
    return StorageObservation("UNKNOWN", errors, streak, recovery)
}
