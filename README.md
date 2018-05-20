# BigQueryIO read / write problems  
A few use cases highlighting problems I've encountered with Apache Beam BigQueryIO

  
  
## Prerequisites

---
1. Create a table in a GCP project like this one. I've snipped relevant parts of the test table schema I worked with from
the bq show command ... `bq show --format=prettyjson <schema>.<table>`

    ```json
      {
      "schema": {
        "fields": [
          {
            "mode": "NULLABLE",
            "name": "PARTITION_DATE",
            "type": "DATE"
          },
          {
            "mode": "NULLABLE",
            "name": "TEXT_FIELD",
            "type": "STRING"
          },
          {
            "mode": "NULLABLE",
            "name": "INT_FIELD",
            "type": "INTEGER"
          },
          {
            "mode": "NULLABLE",
            "name": "LoadDate",
            "type": "DATE"
          },
          {
            "mode": "NULLABLE",
            "name": "RecordSource",
            "type": "STRING"
          }
        ]
      },
      "timePartitioning": {
        "field": "PARTITION_DATE",
        "type": "DAY"
      },
      "type": "TABLE"  
      }
    ```

1. Get a GCS bucket ready, BigQueryIO needs a temp location.
1. Clone the code and `mvn package` it.

## Running it

---
### Using Standard SQL Test cases

1. Run the successful test

    ```bash
    java -cp <path>/bigqueryioproblems/target/bigquery-io-problems-bundled-1.0-SNAPSHOT.jar \
    com.fearlesstg.UsingStandardSQLOrderMatters \
    --runner=DirectRunner \
    --bqTable=<project>:<dataset>.<table> \
    --showItWorking=True \
    --tempLocation=gs://<temp bucket> \
    --project=<project>
    ```

1. Run the unsuccessful test

    ```bash
    java -cp <path>/bigqueryioproblems/target/bigquery-io-problems-bundled-1.0-SNAPSHOT.jar \
    com.fearlesstg.UsingStandardSQLOrderMatters \
    --runner=DirectRunner \
    --bqTable=<project>:<dataset>.<table> \
    --showItWorking=False \
    --tempLocation=gs://<temp bucket> \
    --project=<project>
    ```


### TimePartitioning Test cases

[See the code for the test case definitions](https://github.com/pbrumblay/bigqueryioproblems/blob/master/src/main/java/com/fearlesstg/TimePartitioningTest.java#L40)

```bash
java -cp <path>/bigqueryioproblems/target/bigquery-io-problems-bundled-1.0-SNAPSHOT.jar \
com.fearlesstg.TimePartitioningTest \
--runner=DirectRunner \
--bqTable=<project>:<schema>.<testtable> \
--testCase=<1-5> \
--tempLocation=gs://<temp bucket> \
--project=<project>
```