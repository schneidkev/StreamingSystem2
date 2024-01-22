package Producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

class Producer {

    val producer: KafkaProducer<String, String>
    val bootstrapServers = "localhost:9092"
    val topic = "Sensoren"
    val logger = org.slf4j.LoggerFactory.getLogger("Producer")

    init {
        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        producerProps[ProducerConfig.ACKS_CONFIG] = "1"
        producer = KafkaProducer<String,String>(producerProps)


    }

    fun sendSensorData(data:String){
        val record = ProducerRecord(topic,Random().nextInt(0,2),"SpeedSensor", data) // Generiert eine zufällige Zahl
        producer.send(record) { metadata, exception ->                                                 //zwischen 0 (inklusive) und 2 (exklusive)
            if (exception == null) {
                logger.info("Nachricht gesendet: Topic=${metadata.topic()}, Partition=${metadata.partition()}, Offset=${metadata.offset()}")
            } else {
                logger.error("Fehler beim Senden der Nachricht: ${exception.message}")
            }
        }
    }

    companion object {
        val instance by lazy{ Producer() }
    }
}