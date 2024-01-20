package Consumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.time.Instant
import java.util.*

data class SensorData(val timestamp: String,val sensorID: String, val sensorValue: List<Double>)

class Consumer {
    val topic = "Sensoren"
    val bootstrapServers = "localhost:9092"
    private val consumer: KafkaConsumer<String, String>
    val averageMap: MutableMap<String,MutableMap<String,Double>> = mutableMapOf()
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
        forceToBeginning()
    }

    fun forceToBeginning(){
        while(consumer.poll(Duration.ofSeconds(1)).isEmpty){
            consumer.seekToBeginning(consumer.assignment())
        }
        consumer.seekToBeginning(consumer.assignment())
    }

    fun collectDataOnEventTimestamp(){
        consumer.seekToBeginning(consumer.assignment())
        val sensorDataList = mutableListOf<SensorData>()
        var startTimestamp = Instant.now()
        while(true){
            val records = consumer.poll(Duration.ofSeconds(1))
            for (record in records){
                val parts = record.value().split(";")
                if(sensorDataList.isEmpty()){
                    startTimestamp = Instant.parse(parts[0])
                }
                sensorDataList.add(SensorData(parts[0],parts[1],parts.drop(2).filter{it.isNotEmpty()}.filter{it.toDouble() > 0}.map{it.toDouble() * 3.6}))
                println("Topic: ${record.topic()}, Partition: ${record.partition()}, Offset: ${record.offset()}, Key: ${record.key()}, Value: ${record.value()}")
            }
            if(records.isEmpty) continue
            if(Instant.parse(sensorDataList.sortedBy { it.timestamp }.last().timestamp) >= startTimestamp.plusSeconds(30)){ // damit Daten in der Vergangenheit sowie Daten die noch kommen immer in 30 Sekunden Fenster eingelagert werden können
                //groupTimeWindow(sensorDataList)
                startTimestamp.plusSeconds(30)
                groupTimeWindow(sensorDataList)
                sensorDataList.clear()
            }
        }
    }



    fun groupTimeWindow(list: List<SensorData>){
        val sortedList = list.sortedBy { it.timestamp }
        var startTimestamp = Instant.parse(sortedList.first().timestamp)
        do {
            println(startTimestamp)
            val test = list.filter { startTimestamp.isBefore(Instant.parse(it.timestamp))  && startTimestamp.plusSeconds(30).isAfter(Instant.parse(it.timestamp)) }
            if(test.isEmpty()){
                startTimestamp = Instant.parse(list.sortedBy { it.timestamp }.filter { Instant.parse(it.timestamp) > startTimestamp }.first().timestamp)
                continue
            }
            val average = test.groupBy { it.sensorID }.mapValues {(_, sensorDataList) -> sensorDataList.flatMap { it.sensorValue }.average() }.filter { !it.value.isNaN() }
            averageMap.put(startTimestamp.toString(),average.toMutableMap())
            startTimestamp = startTimestamp.plusSeconds(30)
            println(average)
        } while(Instant.parse(sortedList.last().timestamp)>startTimestamp)
    }

    fun collectDataRealTime(){
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

    fun getSensorAverage(){
        averageMap.map { println(it) }
    }

    companion object {
        val instance by lazy{ Consumer() }
    }
}