package Producer

fun main(args: Array<String>) {
    Consumer.Consumer.instance.collectDataOnEventTimestamp()
}
