package io.fleetcam

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class StoragePolicyTest {
    private val mapper = ObjectMapper()
    private fun sd(errors: Long, free: Long = 800) = mapper.readTree("""{"present":true,"total_bytes":1000,"free_bytes":$free,"write_error_count":$errors}""")
    @Test fun `sustained errors and stable recovery`() {
        var state = storageObservation(sd(0),null)
        assertEquals("HEALTHY",state.state)
        state=storageObservation(sd(1),state); assertEquals("HEALTHY",state.state)
        state=storageObservation(sd(2),state); assertEquals("WRITE_ERROR",state.state)
        state=storageObservation(sd(2),state); assertEquals("WRITE_ERROR",state.state)
        state=storageObservation(sd(2),state); assertEquals("HEALTHY",state.state)
    }
    @Test fun `capacity hysteresis and unknown data`() {
        var state=storageObservation(sd(0,90),null); assertEquals("LOW_CAPACITY",state.state)
        state=storageObservation(sd(0,110),state); assertEquals("LOW_CAPACITY",state.state)
        state=storageObservation(sd(0,130),state); assertEquals("HEALTHY",state.state)
        assertEquals("UNKNOWN",storageObservation(mapper.readTree("{}"),state).state)
        assertEquals("NO_MEDIA",storageObservation(mapper.readTree("{\"present\":false}"),state).state)
        assertEquals("UNKNOWN",storageObservation(sd(5),null).state)
    }
}
