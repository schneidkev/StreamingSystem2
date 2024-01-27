import BeamPipeline.Main;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.io.kafka.KafkaTimestampType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;


public class BeamTests {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testExtractTimestampFunction() {
        KafkaRecord<String, String> record1 = new KafkaRecord<>("test",0,0, 0, KafkaTimestampType.CREATE_TIME, null, "Sensor",  "2024-01-01T00:00:00.000Z;1;32.2;12.20;12.2");
        KafkaRecord<String, String> record2 = new KafkaRecord<>("test",1,1, 0, KafkaTimestampType.CREATE_TIME, null, "Sensor",  "2024-01-02T12:30:00.000Z;7;-12.2;99.20;0.00");

        PCollection<KafkaRecord<String, String>> input = pipeline.apply(Create.of(record1, record2).withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

        PCollection<Instant> output =
                input
                        .apply(WithTimestamps.of(new Main.ExtractTimestampFunction()))
                        .apply(ParDo.of(new DoFn<KafkaRecord<String, String>, Instant>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                c.output(c.timestamp());
                            }
                        }));


        PAssert.that(output).containsInAnyOrder(
                Instant.parse("2024-01-01T00:00:00.000Z"),
                Instant.parse("2024-01-02T12:30:00.000Z"));

        pipeline.run().waitUntilFinish();


    }

    @Test
    public void testFilterFunction() {
        KafkaRecord<String, String> record1 = new KafkaRecord<>("test", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null, "Sensor", "2024-01-01T00:00:00.000Z;1;32.2;12.20;12.2");
        KafkaRecord<String, String> record2 = new KafkaRecord<>("test", 1, 1, 0, KafkaTimestampType.CREATE_TIME, null, "Sensor", "2024-01-02T12:30:00.000Z;7;-12.2;99.20;0.00");
        KafkaRecord<String, String> record3 = new KafkaRecord<>("test", 1, 1, 0, KafkaTimestampType.CREATE_TIME, null, "Sensor", "2024-03-12T12:30:00.000Z;7;");
        KafkaRecord<String, String> record4 = new KafkaRecord<>("test", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null, "Sensor", "2024-01-01T00:00:00.000Z;1;32.2;12.20;32.2");

        PCollection<KafkaRecord<String, String>> input = pipeline.apply(Create.of(record1, record2, record3, record4).withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

        PCollection<KafkaRecord<String, String>> output =
                input
                        .apply(Filter.by(new Main.FilterFunction()));
        PAssert.that(output).containsInAnyOrder(record1, record4);
        pipeline.run().waitUntilFinish();

    }

    @Test
    public void testDivideFunction() {
        KafkaRecord<String, String> record1 = new KafkaRecord<>("test", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null, "Sensor", "2024-01-01T00:00:00.000Z;1;32.2;12.20;12.2");
        KafkaRecord<String, String> record2 = new KafkaRecord<>("test", 1, 1, 0, KafkaTimestampType.CREATE_TIME, null, "Sensor", "2024-01-02T12:30:00.000Z;7;-12.2;99.20;0.00");
        KafkaRecord<String, String> record3 = new KafkaRecord<>("test", 1, 1, 0, KafkaTimestampType.CREATE_TIME, null, "Sensor", "2024-03-12T12:30:00.000Z;7;");
        KafkaRecord<String, String> record4 = new KafkaRecord<>("test", 0, 0, 0, KafkaTimestampType.CREATE_TIME, null, "Sensor", "2024-01-01T00:00:00.000Z;1;32.2;12.20;32.2");

        PCollection<KafkaRecord<String, String>> input = pipeline.apply(Create.of(record1, record2, record3, record4).withCoder(KafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

        PCollection<KV<String, Double>> output =
                input
                        .apply("Divide",ParDo.of(new Main.DivideFunction()));
        PAssert.that(output).containsInAnyOrder(KV.of("1", 32.2), KV.of("1", 12.2), KV.of("1", 12.2), KV.of("1", 32.2));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testConvertMsToKmhFunction(){
        KV<String, Double> kv1 = KV.of("1", 1.0);
        KV<String, Double> kv2 = KV.of("1", 2.0);
        KV<String, Double> kv3 = KV.of("1", 3.0);
        KV<String, Double> kv4 = KV.of("1", 4.0);
        PCollection<KV<String, Double>> input = pipeline.apply(Create.of(kv1, kv2, kv3, kv4).withCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of())));

        PCollection<KV<String, Double>> output =
                input
                        .apply("MStoKMH",ParDo.of(new Main.ConvertMsToKmhFunction()));

        PAssert.that(output).containsInAnyOrder(KV.of("1", 3.6), KV.of("1", 7.2), KV.of("1", 10.8), KV.of("1", 14.4));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testComputeAverageFunction(){
        KV<String, Iterable<Double>> kv1 = KV.of("1", java.util.Arrays.asList(1.0, 2.0, 3.0, 4.0));
        KV<String, Iterable<Double>> kv2 = KV.of("2", java.util.Arrays.asList(1.0, 2.0, 3.0, 4.0));
        KV<String, Iterable<Double>> kv3 = KV.of("3", java.util.Arrays.asList(1.0, 2.0, 3.0, 4.0));
        KV<String, Iterable<Double>> kv4 = KV.of("4", java.util.Arrays.asList(1.0, 2.0, 3.0, 4.0));
        PCollection<KV<String, Iterable<Double>>> input = pipeline.apply(Create.of(kv1, kv2, kv3, kv4));

        PCollection<KV<String, Double>> output =
                input.apply("ComputeAverage", ParDo.of(new Main.ComputeAverageFunction()));

        PAssert.that(output).containsInAnyOrder(KV.of("1", 2.5), KV.of("2", 2.5), KV.of("3", 2.5), KV.of("4", 2.5));
        pipeline.run().waitUntilFinish();
    }
}
