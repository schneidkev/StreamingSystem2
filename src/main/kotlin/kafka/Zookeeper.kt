package kafka

import org.apache.zookeeper.server.ServerConfig
import org.apache.zookeeper.server.ZooKeeperServerMain


fun main(args: Array<String>) {
    val zooKeeperMain = ZooKeeperServerMain()
    val zooKeeperConfig = ServerConfig()
    zooKeeperConfig.parse(arrayOf("2121","dataDir"))
    zooKeeperMain.runFromConfig(zooKeeperConfig)
}