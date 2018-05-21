package com.fearlesstg;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class CannotReadFromPartitionedTable {
    public static void main(String[] args) {

        PipelineOptionsFactory.register(CannotReadFromPartitionedTableOptions.class);
        CannotReadFromPartitionedTableOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
            .as(CannotReadFromPartitionedTableOptions.class);
        Pipeline p = Pipeline.create(options);

        TableReference tableRef = BigQueryHelpers.parseTableSpec(options.getBqTable());
        tableRef.setTableId(tableRef.getTableId() + "$" + options.getPartitionValue());

        //Regardless of whether this should work or not with the table decorator,
        //it should not throw a NullPointerException
        PCollection<TableRow> rowsByPartition = p.apply("Read live data",
            BigQueryIO.readTableRows().from(tableRef));
//        Exception in thread "main" org.apache.beam.sdk.Pipeline$PipelineExecutionException: java.lang.NullPointerException
//        at org.apache.beam.sdk.Pipeline.run(Pipeline.java:317)
//        at org.apache.beam.sdk.Pipeline.run(Pipeline.java:297)
//        at com.fearlesstg.CannotReadFromPartitionedTable.main(CannotReadFromPartitionedTable.java:26)
//        Caused by: java.lang.NullPointerException
//        at org.apache.beam.sdk.io.gcp.bigquery.BigQueryTableSource.getEstimatedSizeBytes(BigQueryTableSource.java:108)
//        at org.apache.beam.runners.direct.BoundedReadEvaluatorFactory$InputProvider.getInitialInputs(BoundedReadEvaluatorFactory.java:207)
//        at org.apache.beam.runners.direct.ReadEvaluatorFactory$InputProvider.getInitialInputs(ReadEvaluatorFactory.java:87)
//        at org.apache.beam.runners.direct.RootProviderRegistry.getInitialInputs(RootProviderRegistry.java:62)

        p.run().waitUntilFinish();
    }

    public interface CannotReadFromPartitionedTableOptions extends PipelineOptions {
        String getPartitionValue();
        void setPartitionValue(String value);

        String getBqTable();
        void setBqTable(String value);
    }
}
