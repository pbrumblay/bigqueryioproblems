package com.fearlesstg;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;


public class TimePartitioningTest {
    public static void main(String[] args) {
        TimePartitioningTestOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
            .as(TimePartitioningTestOptions.class);

        TableSchema tableSchema = BigQueryHelper.loadSchema(options.getBqTable());

        Pipeline p = Pipeline.create(options);

        TableRow row1 = testRow1();
        TableRow row2 = testRow2();
        TableRow row3 = testRow3();

        List<TableRow> allRows = new ArrayList<>();
        allRows.add(row1);
        allRows.add(row2);
        allRows.add(row3);

        PCollection<TableRow> tableRowsToWrite = p.apply(Create.of(allRows));

        //It looks like the common problem in all of these is that CREATE_IF_NEEDED is the only CreateDisposition
        //which works properly with "withTimePartitioning()"

        com.google.api.services.bigquery.model.TimePartitioning timePartitioning = new com.google.api.services.bigquery.model.TimePartitioning();
        switch(options.getTestCase()) {
            case 1: {
                //try it with WRITE_APPEND
                //fails with "Table with field based partitioning must have a schema."
                timePartitioning.setType("DAY");
                timePartitioning.setField("PARTITION_DATE");
                tableRowsToWrite.apply("Fails with missing schema",
                    BigQueryIO.writeTableRows()
                        .to(options.getBqTable()).withTimePartitioning(timePartitioning)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withSchema(tableSchema)
                        .withoutValidation());
            }
            break;
            case 2: {
                //try it with WRITE_TRUNCATE
                //fails with "Table with field based partitioning must have a schema."
                timePartitioning.setType("DAY");
                timePartitioning.setField("PARTITION_DATE");
                tableRowsToWrite.apply("Fails with missing schema",
                    BigQueryIO.writeTableRows()
                        .to(options.getBqTable()).withTimePartitioning(timePartitioning)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withSchema(tableSchema)
                        .withoutValidation());
            }
            break;
            case 3: {
                //try it with CREATE_IF_NEEDED, WRITE_TRUNCATE
                timePartitioning.setType("DAY");
                timePartitioning.setField("PARTITION_DATE");
                tableRowsToWrite.apply("Succeeds",
                    BigQueryIO.writeTableRows()
                        .to(options.getBqTable()).withTimePartitioning(timePartitioning)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withSchema(tableSchema)
                        .withoutValidation());
            }
            break;
            case 4: {
                //try it with CREATE_IF_NEEDED, WRITE_APPEND
                timePartitioning.setType("DAY");
                timePartitioning.setField("PARTITION_DATE");
                tableRowsToWrite.apply("Succeeds",
                    BigQueryIO.writeTableRows()
                        .to(options.getBqTable()).withTimePartitioning(timePartitioning)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withSchema(tableSchema)
                        .withoutValidation());
            }
            break;
            case 5: {
                //Don't set timePartitioning.setType(); assume default id "DAY" per docs. Use WRITE_TRUNCATE.
                //
                //Fails with
                //"Incompatible table partitioning specification.
                //Expects partitioning specification interval(type:day,field:PARTITION_DATE),
                //but input partitioning specification is interval(type:day,field:PARTITION_DATE)"
                //
                //   ^^^^^^
                //   ??? - What?
                //
                //This is unexpected because this is the default for TimePartitioningTest.getType()

                timePartitioning.setField("PARTITION_DATE");
                tableRowsToWrite.apply("Fails with incorrect and misleading error.",
                    BigQueryIO.writeTableRows()
                        .to(options.getBqTable()).withTimePartitioning(timePartitioning)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .withSchema(tableSchema)
                        .withoutValidation());
            }
            break;
        }
        p.run().waitUntilFinish();
    }

    public interface TimePartitioningTestOptions extends PipelineOptions {
        Integer getTestCase();
        void setTestCase(Integer value);

        String getBqTable();
        void setBqTable(String value);
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
