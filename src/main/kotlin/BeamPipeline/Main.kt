package BeamPipeline
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.io.kafka.KafkaRecord
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptors
import org.joda.time.Instant

fun buildPipeline(pipeline: Pipeline) =
        pipeline.apply(
                KafkaIO.read<String, String>()
                .withBootstrapServers("localhost:9092")
                .withTopic("Sensoren")
                .withKeyDeserializer(org.apache.kafka.common.serialization.StringDeserializer::class.java)
                .withValueDeserializer(org.apache.kafka.common.serialization.StringDeserializer::class.java)
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
                Instant.parse(data.split(";")[0])
            }))
            .apply(Window.into(FixedWindows.of(org.joda.time.Duration.standardSeconds(30))))


    fun main(args: Array<String>) {
        val pipeline = Pipeline.create()
        buildPipeline(pipeline)
        pipeline.run().waitUntilFinish()
    }