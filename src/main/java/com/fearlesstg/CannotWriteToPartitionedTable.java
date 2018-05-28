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

