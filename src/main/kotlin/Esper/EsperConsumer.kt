package Esper

import com.espertech.esper.common.client.EventBean
import com.espertech.esper.common.client.configuration.Configuration
import com.espertech.esper.common.client.scopetest.EPAssertionUtil
import com.espertech.esper.compiler.client.CompilerArguments
import com.espertech.esper.compiler.client.EPCompilerProvider
import com.espertech.esper.runtime.client.EPRuntime
import com.espertech.esper.runtime.client.EPRuntimeProvider
import com.espertech.esper.runtime.client.EPStatement
import com.espertech.esper.runtime.client.UpdateListener
import java.time.Duration
import java.time.Instant

val logger = org.slf4j.LoggerFactory.getLogger("EsperConsumer")


class SensorEvent(val timestamp: Instant, val sensorId: Int, val speeds: List<Double>)
class IndividualSpeedEvent(val timestamp: Long, val sensorId: Int, val speed: Double)

class AvgSpeedEvent(val timestamp: Long, val sensorId: Int, val avgSpeed: Double)

class SpeedDropEvent(val timestamp: Long, val sensorId: Int, val minSpeed: Double, val maxSpeed: Double, val speedDrop: Double)

val averageMap: MutableMap<String,MutableMap<String,Double>> = mutableMapOf()

class AveragePrinter() : UpdateListener {

    override fun update(
        newEvents: Array<out EventBean>?,
        oldEvents: Array<out EventBean>?,
        statement: EPStatement?,
        runtime: EPRuntime?
    ) {
        newEvents?.forEach { event ->
            val timestamp = event.get("timestamp") as Long
            val sensorId = event.get("sensorId") as Int
            val speeds = event.get("avgSpeed") as Double
            logger.debug("AverageSpeed: $timestamp;$sensorId;$speeds")

            if (averageMap.containsKey(Instant.ofEpochMilli(timestamp).toString())) {
                averageMap[Instant.ofEpochMilli(timestamp).toString()]?.put(sensorId.toString(), speeds)
            } else {
                averageMap[Instant.ofEpochMilli(timestamp).toString()] = mutableMapOf(sensorId.toString() to speeds)
            }
            getRouteAverageSpeed()
        }
    }

}

fun getRouteAverageSpeed(){
    val routeAverage = averageMap.mapValues { (key, value) -> value.values.sum().let {
            sum ->
        if(value.isNotEmpty()) sum / value.size else 0.0
    }  }
    logger.info("Route Average: $routeAverage")
}

class DropPrinter() : UpdateListener {
    override fun update(
        newEvents: Array<out EventBean>?,
        oldEvents: Array<out EventBean>?,
        statement: EPStatement?,
        runtime: EPRuntime?
    ) {
        newEvents?.forEach { event ->
            val timestamp = event.get("timestamp") as Long
            val sensorId = event.get("sensorId") as Int
            val minSpeed = event.get("minSpeed") as Double
            val maxSpeed = event.get("maxSpeed") as Double
            val speedDrop = event.get("speedDifference") as Double
            logger.info("SpeedDrop: $timestamp;$sensorId;$minSpeed;$maxSpeed;$speedDrop")
        }
    }

}

class SensorEventListener : UpdateListener {
    override fun update(
        newEvents: Array<out EventBean>?,
        oldEvents: Array<out EventBean>?,
        statement: EPStatement?,
        runtime: EPRuntime?
    ) {
        newEvents?.forEach { event ->
            val timestamp = event.get("timestamp") as Instant
            val sensorId = event.get("sensorId") as Int
            val speeds = event.get("speeds") as List<Double>
            if(speeds.isEmpty()){
                return
            }
            speeds.forEach { speed ->
                runtime?.eventService?.sendEventBean(IndividualSpeedEvent(timestamp.toEpochMilli(), sensorId, speed*3.6), "IndividualSpeedEvent") //spliitin into single Events and transform to KMH
            }
        }
    }

}

class EsperConsumer(){
    val runtime:EPRuntime
    init{
        val avgSpeedEPL = "@name ('avgspeed') insert into AvgSpeedEvent select timestamp, sensorId, avg(speed) as avgSpeed from IndividualSpeedEvent.win:ext_timed(timestamp, 30 sec) group by sensorId; \n"
        val cleanData = "@name ('cleandata') select * from SensorEvent(speeds.allOf(v => v > 0)); \n"
        val trafficJamEPL = "@name ('speeddrop') insert into SpeedDropEvent SELECT timestamp, sensorId, min(avgSpeed) as minSpeed, max(avgSpeed) as maxSpeed, max(avgSpeed) - min(avgSpeed) as speedDifference " +
                "FROM AvgSpeedEvent.win:ext_timed(timestamp, 30 sec) group by sensorId having max(avgSpeed) - min(avgSpeed) > 20; \n"

        val configuration = Configuration()
        configuration.common.addEventType("SensorEvent", SensorEvent::class.java.name)
        configuration.common.addEventType("AvgSpeedEvent", AvgSpeedEvent::class.java.name)
        configuration.common.addEventType("IndividualSpeedEvent", IndividualSpeedEvent::class.java.name)
        val compiler = EPCompilerProvider.getCompiler()
        val args = CompilerArguments(configuration)
        val epCompiled = compiler.compile(cleanData + avgSpeedEPL + trafficJamEPL , args)

        runtime = EPRuntimeProvider.getDefaultRuntime(configuration)
        runtime.initialize()

        val deployment = runtime.deploymentService.deploy(epCompiled)
        val statement2 = runtime.deploymentService.getStatement(deployment.deploymentId, "cleandata")
        val statement = runtime.deploymentService.getStatement(deployment.deploymentId, "avgspeed")
        val statement3 = runtime.deploymentService.getStatement(deployment.deploymentId, "speeddrop")

        statement.addListener(AveragePrinter())
        statement2.addListener(SensorEventListener())
        statement3.addListener(DropPrinter())
    }

    fun sendData(sensordata:String){
        val parts = sensordata.split(";")
        if(parts[2].isNotEmpty()){
            val sensorData = SensorEvent(Instant.parse(parts[0]), parts[1].toInt(), parts.drop(2).map { it.toDouble() })
            runtime.eventService.sendEventBean(sensorData, "SensorEvent")
        }
    }
    companion object {
        val instance by lazy{ EsperConsumer() }
    }
}
