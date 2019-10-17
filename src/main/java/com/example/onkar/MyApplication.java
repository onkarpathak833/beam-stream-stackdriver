package com.example.onkar;

import com.example.onkar.io.KafkaAccessor;
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class MyApplication {
//    private static final String GCS_LOCATION = "gs://beam-datasets-tw/df/";
public static final String GCP_API_KEY = "C:\\Data\\etl_sa_key.json";
    public static void main(String[] args) {
        GoogleCredentials credentials = CredentialsManager.loadGoogleCredentials(GCP_API_KEY);
        KafkaAccessor accessor = new KafkaAccessor();
        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .create()
                .as(DataflowPipelineOptions.class);


        pipelineOptions.setProject("project1-186407");
        System.setProperty("BUCKET_NAME","gs://beam-datasets-tw/df");
        String bucketName = System.getProperty("BUCKET_NAME");
        pipelineOptions.setStagingLocation("gs://beam-datasets-tw/df");
        pipelineOptions.setRunner(DataflowRunner.class);
        pipelineOptions.setGcpTempLocation(bucketName);
        FileSystems.setDefaultPipelineOptions(pipelineOptions);
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        PCollection<String> records = KafkaAccessor.readFromKafka(pipeline);
        KafkaAccessor.writeToGCS(records);
        pipeline.run().waitUntilFinish();
    }
}
