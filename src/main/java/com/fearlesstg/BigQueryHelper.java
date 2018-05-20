package com.fearlesstg;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BigQueryHelper {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryHelper.class);

    public static TableSchema loadSchema(String destination) {

        LOG.info("Destination: " + destination);
        String[] projectTheRest = destination.split(":");
        String project = projectTheRest[0];
        LOG.info("Project: " + project);
        String[] dataSetTable = projectTheRest[1].split("\\.");
        String dataset = dataSetTable[0];
        String table = dataSetTable[1];

        HttpTransport transport = new NetHttpTransport();
        JsonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential;
        try {
            credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (credential.createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }
        Bigquery bq = new Bigquery.Builder(transport, jsonFactory, credential)
            .setApplicationName("DataStageImport").build();

        Table t;
        try {
            t = bq.tables().get(project, dataset, table).execute();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return t.getSchema();
    }
}
