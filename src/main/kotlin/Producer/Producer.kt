package Producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

class Producer {

    val producer: KafkaProducer<String, String>
    val bootstrapServers = "localhost:9092"
    val topic = "Sensoren"

    init {
        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        producerProps[ProducerConfig.ACKS_CONFIG] = "1"
        producer = KafkaProducer<String,String>(producerProps)


    }

    fun sendSensorData(data:String){
        val record = ProducerRecord(topic,0,"Sensor", data)
        producer.send(record) { metadata, exception ->
            if (exception == null) {
                println("Nachricht gesendet: Topic=${metadata.topic()}, Partition=${metadata.partition()}, Offset=${metadata.offset()}")
            } else {
                println("Fehler beim Senden der Nachricht: ${exception.message}")
            }
        }
    }

    companion object {
        val instance by lazy{ Producer() }
    }
}