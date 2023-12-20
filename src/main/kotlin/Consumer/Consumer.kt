package Consumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.*

data class SensorData(val timestamp: String,val sensorID: String, val sensorValue: List<Double>)

class Consumer {
    val topic = "Sensoren"
    val bootstrapServers = "localhost:9092"
    val consumer: KafkaConsumer<String, String>

    init{
        val props = Properties()
        props.setProperty("bootstrap.servers", bootstrapServers)
        props.setProperty("group.id", "test")
        props.setProperty("enable.auto.commit", "true")
        props.setProperty("auto.commit.interval.ms", "1000")
        props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
        props.setProperty("group.instance.id", "numberMovesList")
        props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")

        consumer= KafkaConsumer(props)
        consumer.subscribe(mutableListOf(topic))
    }

    fun collectData(){
        while(true){
            consumer.paused()
            Thread.sleep(Duration.ofSeconds(30))
            consumer.resume(consumer.assignment())
            val sensorDataList = mutableListOf<SensorData>()
            val records = consumer.poll(Duration.ofSeconds(30))
            for (record in records){
                val parts = record.value().split(";")
                sensorDataList.add(SensorData(parts[0],parts[1],parts.drop(2).map { it.toDouble() }))
                println("Topic: ${record.topic()}, Partition: ${record.partition()}, Offset: ${record.offset()}, Key: ${record.key()}, Value: ${record.value()}")
            }
            val group = sensorDataList.groupBy { it.sensorID }
            val average = sensorDataList.groupBy { it.sensorID }.mapValues {(_, sensorDataList) -> sensorDataList.flatMap { it.sensorValue }.average() }
            println(average)
        }
    }

    companion object {
        val instance by lazy{ Consumer() }
    }
}