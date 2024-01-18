package Esper

import com.espertech.esper.common.client.EPCompiled
import com.espertech.esper.common.client.EventBean
import com.espertech.esper.common.client.configuration.Configuration
import com.espertech.esper.compiler.client.CompilerArguments
import com.espertech.esper.compiler.client.EPCompiler
import com.espertech.esper.compiler.client.EPCompilerProvider
import com.espertech.esper.runtime.client.EPRuntime
import com.espertech.esper.runtime.client.EPRuntimeProvider
import com.espertech.esper.runtime.client.EPStatement
import com.espertech.esper.runtime.client.UpdateListener
import java.time.Instant


class SensorEvent(val timestamp: Instant, val sensorId: Int, val speeds: List<Double>)
class AvgSpeedEvent(val sensorId: Int, val avgSpeed: Double)

class SpeedDropEvent(val sensorId: Int, val speedDrop: Double)


class Printer() : UpdateListener {
    override fun update(
        newEvents: Array<out EventBean>?,
        oldEvents: Array<out EventBean>?,
        statement: EPStatement?,
        runtime: EPRuntime?
    ) {
        newEvents?.forEach { event ->
            val sensorId = event.get("sensorId") as Int
            val speeds = event.get("speeds") as List<Double>
            println("Sensor ID: $sensorId, Speeds: $speeds")
        }
    }

}

fun main (args: Array<String>) {
    val cleanDataEPL = "@name ('cleanData') select * from SensorEvent(speeds.allOf(v => v > 0))"
    //val avgSpeedEPL = "insert into AvgSpeedEvent select sensorId, avg(speeds) as avgSpeed from SensorEvent.win:time(30 sec) group by sensorId"
    val configuration = Configuration()
    configuration.common.addEventType("SensorEvent", SensorEvent::class.java.name)
    configuration.common.addEventType("AvgSpeedEvent", AvgSpeedEvent::class.java.name)
    val compiler = EPCompilerProvider.getCompiler()
    val args = CompilerArguments(configuration)
    val epCompiled = compiler.compile(cleanDataEPL , args)
    val runtime = EPRuntimeProvider.getDefaultRuntime(configuration)
    runtime.initialize()
    val deployment = runtime.deploymentService.deploy(epCompiled)
    val statement = runtime.deploymentService.getStatement(deployment.deploymentId, "cleanData")
    statement.addListener(Printer())
}