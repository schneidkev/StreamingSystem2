package beamPipeline;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Main {

    static Map<String, Map<String,Double>> averageMap = new HashMap<>();


    public static void buildPipeline(Pipeline pipeline) {
        pipeline.apply(
                        KafkaIO.<String, String>read()
                                .withBootstrapServers("localhost:9092")
                                .withTopic("Sensoren")
                                .withKeyDeserializer(StringDeserializer.class)
                                .withValueDeserializer(StringDeserializer.class)
                )
                .apply(WithTimestamps.of(new ExtractTimestampFunction())
                        .withAllowedTimestampSkew(Duration.standardSeconds(5)))
                .apply(Filter.by(new FilterFunction()))
                .apply("Divide",ParDo.of(new DivideFunction()))
                .apply("MStoKMH",ParDo.of(new ConvertMsToKmhFunction()))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(30))))
                .apply(GroupByKey.create())
                .apply("ComputeAverage", ParDo.of(new ComputeAverageFunction()));
    }
    public static void main(String[] args) {
        System.out.println("Starting pipeline...");
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);

        buildPipeline(pipeline);

        pipeline.run().waitUntilFinish();
    }

    public static class ExtractTimestampFunction implements SerializableFunction<KafkaRecord<String, String>, Instant> {
        @Override
        public Instant apply(KafkaRecord<String, String> input) {
            return Instant.parse(Objects.requireNonNull(input.getKV().getValue()).split(";")[0]);
        }
    }

    public static class FilterFunction implements SerializableFunction<KafkaRecord<String, String>, Boolean> {
        @Override
        public Boolean apply(KafkaRecord<String, String> input) {
            String[] parts = Objects.requireNonNull(input.getKV().getValue()).split(";");
            return parts.length > 2 && java.util.Arrays.stream(parts).skip(2)
                    .allMatch(part -> !part.isEmpty() && Double.parseDouble(part) > 0);
        }
    }

    public static class DivideFunction extends DoFn<KafkaRecord<String, String>, KV<String, Double>> {
        @ProcessElement
        public void processElement(@Element KafkaRecord<String, String> element, OutputReceiver<KV<String, Double>> out) {
            String[] parts = Objects.requireNonNull(element.getKV().getValue()).split(";");
            String id = parts[1];
            java.util.stream.Stream.of(parts).skip(2)
                    .mapToDouble(Double::parseDouble)
                    .forEach(value -> out.output(KV.of(id, value)));
        }
    }

    public static class ConvertMsToKmhFunction extends DoFn<KV<String, Double>, KV<String, Double>> {
        @ProcessElement
        public void processElement(@Element KV<String, Double> element, OutputReceiver<KV<String, Double>> out) {
            String id = element.getKey();
            double value = element.getValue() * 3.6;
            out.output(KV.of(id, value));
        }
    }

    public static class ComputeAverageFunction extends DoFn<KV<String, Iterable<Double>>, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, Iterable<Double>> element = c.element();
            List<Double> values = (List<Double>) element.getValue();
            Double average = values.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
            System.out.println("Average for " + element.getKey() + ": " + average);
            c.output(KV.of(element.getKey(), average));

            Map<String, Double> currentMap = averageMap.get(c.timestamp().toString());
            if (currentMap != null) {
                currentMap.put(element.getKey(), average);
            } else {
                currentMap = new HashMap<>();
                currentMap.put(element.getKey(), average);
                averageMap.put(c.timestamp().toString(), currentMap);
            }
            getRouteAverageSpeed();
        }
    }
    public static void getRouteAverageSpeed() {
        Map<String, Double> routeAverage = new HashMap<>();
        for (Map.Entry<String, Map<String, Double>> entry : averageMap.entrySet()) {
            String key = entry.getKey();
            Map<String, Double> value = entry.getValue();
            double sum = value.values().stream().mapToDouble(Double::doubleValue).sum();
            double average = value.isEmpty() ? 0.0 : sum / value.size();
            routeAverage.put(key, average);
        }
        System.out.println("Route Average:");
        for (Map.Entry<String, Double> entry : routeAverage.entrySet()) {
            String key = entry.getKey();
            double average = entry.getValue();
            System.out.print("Time: " + key + ", Average: " + average + " |");
        }
        System.out.println("-------------------------------------------------");
    }
}