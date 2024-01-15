package Producer

fun main(args: Array<String>) {
    Thread.sleep(1000)
    Consumer.Consumer.instance.collectDataOnEventTimestamp()
}
