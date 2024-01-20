package BeamPipeline;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.api.SystemParameterOrBuilder;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;

import java.util.Objects;

public class Main {

    public static void buildPipeline(Pipeline pipeline) {
        pipeline.apply(
                        KafkaIO.<String, String>read()
                                .withBootstrapServers("localhost:9092")
                                .withTopic("Sensoren")
                                .withKeyDeserializer(StringDeserializer.class)
                                .withValueDeserializer(StringDeserializer.class)
                )
                .apply(WithTimestamps.of(new SerializableFunction<KafkaRecord<String,String>, Instant>() {
                    @Override
                    public Instant apply(KafkaRecord<String, String> input) {
                        return Instant.parse(Objects.requireNonNull(input.getKV().getValue()).split(";")[0]);
                    }
                }).withAllowedTimestampSkew(Duration.standardSeconds(1)))
                .apply(Filter.by(new SerializableFunction<KafkaRecord<String, String>, Boolean>() {
                    @Override
                    public Boolean apply(KafkaRecord<String, String> input) {
                        String[] parts = Objects.requireNonNull(input.getKV().getValue()).split(";");
                        return parts.length > 2 && java.util.Arrays.stream(parts).skip(2)
                                .allMatch(part -> !part.isEmpty() && Double.parseDouble(part) > 0);
                    }
                }))
                .apply("Divide",ParDo.of(new DoFn<KafkaRecord<String, String>, KV<String, Double>>() {
                    @ProcessElement
                    public void processElement(@Element KafkaRecord<String, String> element, OutputReceiver<KV<String, Double>> out) {
                        String[] parts = Objects.requireNonNull(element.getKV().getValue()).split(";");
                        String id = parts[1];
                        java.util.stream.Stream.of(parts).skip(2)
                                .mapToDouble(Double::parseDouble)
                                .forEach(value -> out.output(KV.of(id, value)));
                    }
                })).apply("MStoKMH",ParDo.of(new DoFn<KV<String, Double>, KV<String, Double>>() {
                    @ProcessElement
                    public void processElement(@Element KV<String, Double> element, OutputReceiver<KV<String, Double>> out) {
                        String id = element.getKey();
                        double value = element.getValue() * 3.6;
                        out.output(KV.of(id, value));
                    }
                }))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(30))))
                .apply(GroupByKey.create())
                .apply("ComputeAverage", ParDo.of(new DoFn<KV<String, Iterable<Double>>, KV<String, Double>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, Iterable<Double>> element = c.element();
                        int count = 0;
                        double sum = 0;
                        for (Double value : element.getValue()) {
                            sum += value;
                            count++;
                        }
                        double average = count > 0 ? sum / count : 0;
                        System.out.println(element);
                        System.out.println("Average for " + element.getKey() + ": " + average);
                        c.output(KV.of(element.getKey(), average));
                    }
                }));
    }
    public static void main(String[] args) {
        System.out.println("Starting pipeline...");
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);

        buildPipeline(pipeline);

        pipeline.run().waitUntilFinish();
    }
}