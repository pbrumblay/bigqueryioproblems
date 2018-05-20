package com.fearlesstg;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;


public class UsingStandardSQLOrderMatters {
    public static void main(String[] args) {
        UsingStandardSQLOrderMattersOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
            .as(UsingStandardSQLOrderMattersOptions.class);
        Pipeline p = Pipeline.create(options);

        StringBuilder query = new StringBuilder();
        query.append("SELECT * FROM `");
        query.append(options.getBqTable().replace(':', '.'));
        query.append("`");
        query.append(" WHERE PARTITION_DATE IN ('2018-01-01', '2018-02-02')");

        if(options.getShowItWorking()) {
            p.apply("Read rows will succeed", BigQueryIO.readTableRows().fromQuery(query.toString()).usingStandardSql());
        } else {
            p.apply("Read rows will fail", BigQueryIO.readTableRows().usingStandardSql().fromQuery(query.toString()));
        }

        p.run().waitUntilFinish();
    }

    public interface UsingStandardSQLOrderMattersOptions extends PipelineOptions {
        Boolean getShowItWorking();
        void setShowItWorking(Boolean value);

        String getBqTable();
        void setBqTable(String value);
    }
}
