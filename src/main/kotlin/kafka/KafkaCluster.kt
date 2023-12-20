package kafka

import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import org.apache.kafka.common.utils.Time
import scala.Some

object KafkaCluster {
    @JvmStatic
    fun main(args: Array<String>) {
        val confmap: MutableMap<String, String> = HashMap()
        confmap["zookeeper.connect"] = "localhost:2121"
        confmap["offsets.topic.replication.factor"] = "1"
        val config = KafkaConfig(confmap)
        val kafka = KafkaServer(config, Time.SYSTEM, Some("Main"),true)
        kafka.startup()
    }
}