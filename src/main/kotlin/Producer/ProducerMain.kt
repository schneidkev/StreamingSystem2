package Producer

import java.time.Duration
import java.time.Instant
import java.util.*

fun main(args: Array<String>) {

    val goalTime = Duration.ofSeconds(100).toMillis() + System.currentTimeMillis()
    while(System.currentTimeMillis()<goalTime) {
        Producer.instance.sendSensorData(generateTestData(10, 3, 0.0, 100.0,1,2, 0.1, 1.0))
    }
}

fun generateTestData(sensorAmount: Int, amount:Int, minimum: Double, maximum: Double, m1:Int, m2:Int, negativeChance: Double = 0.3, emptyChance: Double = 0.1):String {
    val random = Random()
    Thread.sleep(random.nextInt(m1*1000,m2*1000).toLong())
    val timestamp = Instant.now().toString()
    val id = random.nextInt(sensorAmount).toString()
    if (random.nextDouble() < emptyChance) {
        return "$timestamp;$id;"
    }
    val values = List(amount) {
        String.format(
            Locale.US,
            "%.2f",
            if (random.nextDouble() < negativeChance) {
                random.nextDouble(-maximum, -minimum)
            } else {
                random.nextDouble(minimum, maximum)
            }
        )
    }
    return "$timestamp;$id;${values.joinToString(";")}"

}