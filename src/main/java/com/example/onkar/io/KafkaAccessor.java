package com.example.onkar.io;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAccessor {

    static Logger logger = LoggerFactory.getLogger(KafkaAccessor.class);
//    private static final String GCS_LOCATION = "gs://${BUCKET_NAME}/df/";

    public static PCollection<String> readFromKafka(Pipeline pipeline) {
        PCollection<KafkaRecord<String, String>> kafkaCollection = pipeline.apply(KafkaIO.<String, String>read()
                .withBootstrapServers("104.196.143.63:9092")
                .withTopic("test")
                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object) "latest"))
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class));

        return kafkaCollection.apply(ParDo.of(new DoFn<KafkaRecord<String, String>, String>() {

            @ProcessElement
            public void processElement(ProcessContext processContext) {
                KafkaRecord<String, String> record = processContext.element();
                String value = record.getKV().getValue();
                logger.info(" Record Data Here Topic Name: ,Key : ,and Value: , {} {} {}", record.getTopic(), record.getKV().getKey(), record.getKV().getValue());
                processContext.output(value);
            }
        }));

    }

    public static void writeToGCS(PCollection<String> records) {
        String bucketName = System.getProperty("BUCKET_NAME");
        records.apply(Window.<String>
                into(FixedWindows.of(Duration.standardSeconds(4))))
                .apply(TextIO.write()
                        .withWindowedWrites()
                        .withNumShards(3)
                        .to("gs://beam-datasets-tw/df"));
//        records.apply(TextIO.write().to("gs://beam-datasets-tw/df"));
    }
}
