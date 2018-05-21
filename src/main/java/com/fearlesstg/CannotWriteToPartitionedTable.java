package com.fearlesstg;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;


import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("Duplicates")
public class CannotWriteToPartitionedTable {


    public static void main(String[] args) {
        PipelineOptionsFactory.register(CannotWriteToPartitionedTableOptions.class);
        CannotWriteToPartitionedTableOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
            .as(CannotWriteToPartitionedTableOptions.class);

        Pipeline p = Pipeline.create(options);

        TableRow row1 = testRow1();
        TableRow row2 = testRow2();
        TableRow row3 = testRow3();

        List<TableRow> allRows = new ArrayList<>();
        allRows.add(row1);
        allRows.add(row2);
        allRows.add(row3);

        PCollection<TableRow> tableRowsToWrite = p.apply(Create.of(allRows));

        //Try to replace target partition using one of two methods. Both fail with
        //"Invalid table ID <table>$<partition>. Table IDs must be alphanumeric (plus underscores) and must be at
        // most 1024 characters long. Also, Table decorators cannot be used."
        //
        // Should work according to ...
        // https://stackoverflow.com/questions/47351578/create-dynamic-side-outputs-in-apache-beam-dataflow#comment81668927_47351578
        // and https://stackoverflow.com/a/43505535
        // OR
        // https://shinesolutions.com/2017/12/05/fun-with-serializable-functions-and-dynamic-destinations-in-cloud-dataflow/
        if(options.getUseDynamicDestinations() != null && options.getUseDynamicDestinations()) {
            tableRowsToWrite.apply("Write to raw data tables", BigQueryIO.writeTableRows()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .to(new PartitionedTableRef(options.getBqTable(), options.getPartitionValue())));
        } else {
            tableRowsToWrite.apply("Write to raw data tables", BigQueryIO.writeTableRows()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .to(new UsingSerializableFunction(options.getBqTable(), options.getPartitionValue())));
        }


        p.run().waitUntilFinish();
    }

    public interface CannotWriteToPartitionedTableOptions extends PipelineOptions {
        String getPartitionValue();
        void setPartitionValue(String value);

        String getBqTable();
        void setBqTable(String value);

        Boolean getUseDynamicDestinations();
        void setUseDynamicDestinations(Boolean value);
    }

    private static TableRow testRow1() {
        TableRow tr = new TableRow();
        tr.set("PARTITION_DATE", "2018-01-01");
        tr.set("TEXT_FIELD", "This is some data for 1");
        tr.set("INT_FIELD", "1111");
        tr.set("LoadDate", "2018-01-01");
        tr.set("RecordSource", "somesource1");
        return tr;
    }

    private static TableRow testRow2() {
        TableRow tr = new TableRow();
        tr.set("PARTITION_DATE", "2018-02-02");
        tr.set("TEXT_FIELD", "This is some data for 2");
        tr.set("INT_FIELD", "2222");
        tr.set("LoadDate", "2018-01-01");
        tr.set("RecordSource", "somesource2");
        return tr;
    }

    private static TableRow testRow3() {
        TableRow tr = new TableRow();
        tr.set("PARTITION_DATE", "2018-03-03");
        tr.set("TEXT_FIELD", "This is some data for 3");
        tr.set("INT_FIELD", "3333");
        tr.set("LoadDate", "2018-01-01");
        tr.set("RecordSource", "somesource3");
        return tr;
    }
}

//From: https://stackoverflow.com/questions/47351578/create-dynamic-side-outputs-in-apache-beam-dataflow#comment81668927_47351578
//AND: https://stackoverflow.com/a/43505535
class PartitionedTableRef extends DynamicDestinations<TableRow, String> {

    private final String bqDestination;
    private final String partitionedColumnValue;

    public PartitionedTableRef(String bqDestination, String partitionedColumnValue) {
        this.bqDestination = bqDestination;
        this.partitionedColumnValue = partitionedColumnValue;
    }

    @Override
    public String getDestination(ValueInSingleWindow<TableRow> element) {
        return String.format("%s$%s", this.bqDestination, this.partitionedColumnValue);
    }

    @Override
    public TableDestination getTable(String destination) {
        TimePartitioning DAY_PARTITION = new TimePartitioning().setType("DAY");
        return new TableDestination(destination, null, DAY_PARTITION);
    }

    @Override
    public TableSchema getSchema(String destination) {
        return BigQueryHelper.loadSchema(this.bqDestination);
    }
}

//From: https://shinesolutions.com/2017/12/05/fun-with-serializable-functions-and-dynamic-destinations-in-cloud-dataflow/
class UsingSerializableFunction implements SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination> {
    private final String bqDestination;
    private final String partitionedColumnValue;

    public UsingSerializableFunction(String bqDestination, String partitionedColumnValue) {
        this.bqDestination = bqDestination;
        this.partitionedColumnValue = partitionedColumnValue;
    }

    public TableDestination apply(ValueInSingleWindow<TableRow> element) {
        return new TableDestination(this.bqDestination + "$" + this.partitionedColumnValue, null);
    }
}

