package BeamPipeline
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.io.kafka.KafkaRecord
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.KV
import org.apache.kafka.common.serialization.StringDeserializer
import org.joda.time.Duration
import org.joda.time.Instant

fun buildPipeline(pipeline: Pipeline) =
        pipeline.apply(
                KafkaIO.read<String, String>()
                .withBootstrapServers("localhost:9092")
                .withTopic("Sensoren")
                .withKeyDeserializer(StringDeserializer::class.java)
                .withValueDeserializer(StringDeserializer::class.java)
                    .withConsumerConfigUpdates(mapOf("auto.offset.reset" to "earliest"))
            ).apply(Filter.by(object : SerializableFunction<KafkaRecord<String, String>, Boolean> {
            override fun apply(input: KafkaRecord<String, String>): Boolean{
                return input.kv.value.split(";").drop(2)[0] != ""
            }
        }))
            .apply(Filter.by(object: SerializableFunction<KafkaRecord<String, String>, Boolean> {
                override fun apply(input: KafkaRecord<String, String>): Boolean{
                    return input.kv.value.split(";").drop(2).all { it.toDouble() > 0  }
                }
            }))
            .apply(ParDo.of(object: DoFn<KafkaRecord<String, String>, String>(){
                @ProcessElement
                fun processElement(@Element element: KafkaRecord<String, String>, out: OutputReceiver<String>){
                    val parts = element.kv.value.split(";")
                    val unmodified = parts.take(2).joinToString(";")
                    val modified = parts.drop(2).map { it.toDouble() * 3.6 }.joinToString(";")
                    out.output("$unmodified;$modified")
                }
            }))
            .apply(WithTimestamps.of(SerializableFunction<String, Instant>{
                data ->
                println("COOOOOOL")
                Instant.parse(data.split(";")[0])
            }).withAllowedTimestampSkew(Duration(Long.MAX_VALUE)))
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(30))))
            .apply(ParDo.of(object: DoFn<String, KV<Int, java.util.List<Double>>>(){
                @ProcessElement
                fun processElement(@Element element: String, out: OutputReceiver<KV<Int, java.util.List<Double>>>){
                    val parts = element.split(";")
                    val id = parts[1].toInt()
                    val values = parts.drop(2).map { it.toDouble() }
                    println("Group: $values")
                    out.output(KV.of(id, values as java.util.List<Double>))
                }
            }))
            .apply(GroupByKey.create())
            .apply(ParDo.of(object: DoFn<KV<Int, Iterable<java.util.List<Double>>>, String>(){
                @ProcessElement
                fun processElement(@Element element: KV<Int, Iterable<java.util.List<Double>>>, out: OutputReceiver<String>){
                    println("bitee geh: ${element}")
                }
            }))



fun main(args: Array<String>) {
        val pipeline = Pipeline.create()
        val schema = buildPipeline(pipeline)
    val test = object: DoFn<KV<Int, Iterable<List<Double>>>, String>(){
        @ProcessElement
        fun processElement(@Element element: KV<Int, Iterable<List<Double>>>, out: OutputReceiver<String>){

        }
    }
    val signature = DoFnSignatures.getSignature(test.javaClass)
    val processElementMethod = signature.processElement()

    processElementMethod.getSchemaElementParameters()?.forEach { println(it) }

    pipeline.run().waitUntilFinish()

}