import Esper.EsperConsumer
import Esper.SensorEvent
import com.espertech.esper.runtime.client.EPRuntime
import org.junit.jupiter.api.BeforeEach
import java.time.Duration
import java.time.Instant
import kotlin.test.Test

class EsperConsumerTest {
    private lateinit var runtime: EPRuntime
    @BeforeEach
    fun setUp(){
        runtime = EsperConsumer.instance.runtime
        Thread.sleep(3000)
    }

    @Test
    fun testFixedWindow(){
        val goalTime = Duration.ofSeconds(30).toMillis() + System.currentTimeMillis()

        while(System.currentTimeMillis()<goalTime){
            val sensorData = SensorEvent(Instant.now(), 1, listOf(1.0,1.0,1.0))
            runtime.eventService.sendEventBean(sensorData, "SensorEvent")
            Thread.sleep(1000)
        }
        val sensorData = SensorEvent(Instant.now(), 1, listOf(2.0,2.0,2.0))
        runtime.eventService.sendEventBean(sensorData, "SensorEvent")
    }

    @Test
    fun testWindowSliding(){
        val goalTime = Duration.ofSeconds(30).toMillis() + System.currentTimeMillis()
        val sensorData = SensorEvent(Instant.now(), 1, listOf(1.0,1.0,1.0))
        runtime.eventService.sendEventBean(sensorData, "SensorEvent")
        while(System.currentTimeMillis()<goalTime){
            Thread.sleep(1000)
        }
        val sensorData2 = SensorEvent(Instant.now(), 1, listOf(2.0,2.0,2.0))
        runtime.eventService.sendEventBean(sensorData2, "SensorEvent")
    }

    @Test
    fun testDataFilter(){
        val goalTime = Duration.ofSeconds(32).toMillis() + System.currentTimeMillis()
        while(System.currentTimeMillis()<goalTime){
            val sensorData = SensorEvent(Instant.now(), 1, listOf())
            val sensordata2 = SensorEvent(Instant.now(), 1, listOf(-1.0,1.0,1.0))
            val sensordata3 = SensorEvent(Instant.now(), 1, listOf(1.0,1.0,1.0))
            runtime.eventService.sendEventBean(sensorData, "SensorEvent")
            runtime.eventService.sendEventBean(sensordata2, "SensorEvent")
            runtime.eventService.sendEventBean(sensordata3, "SensorEvent")
            Thread.sleep(1000)
        }
    }

    @Test
    fun testSpeedDropEvent(){
        val goalTime = Duration.ofSeconds(32).toMillis() + System.currentTimeMillis()
        while(System.currentTimeMillis()<goalTime){
            val sensorData = SensorEvent(Instant.now(), 1, listOf(1.0))
            val sensordata2 = SensorEvent(Instant.now(), 1, listOf(20.0))
            runtime.eventService.sendEventBean(sensorData, "SensorEvent")
            runtime.eventService.sendEventBean(sensordata2, "SensorEvent")
            Thread.sleep(1000)
        }
    }

    @Test
    fun testSpeedDropEventBySensorId(){
        val goalTime = Duration.ofSeconds(32).toMillis() + System.currentTimeMillis()
        while(System.currentTimeMillis()<goalTime){
            val sensorData = SensorEvent(Instant.now(), 1, listOf(1.0))
            val sensordata2 = SensorEvent(Instant.now(), 2, listOf(20.0))
            runtime.eventService.sendEventBean(sensorData, "SensorEvent")
            runtime.eventService.sendEventBean(sensordata2, "SensorEvent")
            Thread.sleep(1000)
        }
    }
}