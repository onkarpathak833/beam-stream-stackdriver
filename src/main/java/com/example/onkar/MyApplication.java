package com.example.onkar;

import com.example.onkar.io.KafkaAccessor;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class MyApplication {
    private static final String GCS_LOCATION = "gs://beam-datasets/tw/df";
    public static void main(String[] args) {
        KafkaAccessor accessor = new KafkaAccessor();
        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .create()
                .as(DataflowPipelineOptions.class);

        pipelineOptions.setProject("project1-186407");
        pipelineOptions.setStagingLocation(GCS_LOCATION);
        pipelineOptions.setRunner(DataflowRunner.class);

        Pipeline pipeline = Pipeline.create(pipelineOptions);
        PCollection<String> records = KafkaAccessor.readFromKafka(pipeline);
        KafkaAccessor.writeToKafka(records);
        pipeline.run().waitUntilFinish();
    }
}
