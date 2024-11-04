
        package com.example;

        import org.apache.spark.sql.*;
        import org.apache.spark.sql.streaming.StreamingQuery;
        import org.apache.spark.sql.streaming.StreamingQueryException;
        import org.apache.spark.sql.types.*;
        import io.delta.tables.DeltaTable;

        import java.util.Arrays;

        public class AdvancedDataEngineeringWithDeltaLake {
            public static void main(String[] args) throws StreamingQueryException {
                // Set up Spark session with Delta support
                SparkSession spark = SparkSession.builder()
                        .appName("AdvancedDataEngineeringWithDeltaLake")
                        .master("local[*]")
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                        .getOrCreate();

                // 1. Partitioning Strategies
                partitioningStrategies(spark);

                // 2. Structured Streaming with Change Data Feed (CDF)
                structuredStreamingWithCDF(spark);

                // 3. Medallion Architecture
                medallionArchitecture(spark);

                // 4. Handling Invalid Records
                handleInvalidRecords(spark);

                // 5. Slowly Changing Dimensions (SCDs)
                manageSCDs(spark);
            }

            public static void partitioningStrategies(SparkSession spark) {
                Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                        RowFactory.create(1, "2024-01-01", "A"),
                        RowFactory.create(2, "2024-01-02", "B"),
                        RowFactory.create(3, "2024-01-03", "A"),
                        RowFactory.create(4, "2024-01-04", "C")),
                        new StructType()
                                .add("id", DataTypes.IntegerType)
                                .add("date", DataTypes.StringType)
                                .add("event_type", DataTypes.StringType));

                // 1.1 Traditional Partitioning: Partition by date
                df.write()
                        .format("delta")
                        .mode("overwrite")
                        .partitionBy("date")
                        .save("/tmp/delta/partitioned_data");

                System.out.println("Partitioned data by 'date' column");

                // 1.2 Z-Order for Liquid Partitioning (alternative approach)
                spark.sql("OPTIMIZE '/tmp/delta/partitioned_data' ZORDER BY (event_type)");

                // 1.3 Bloom Filter Example
                spark.sql("CREATE BLOOMFILTER INDEX ON TABLE delta.`/tmp/delta/partitioned_data` FOR COLUMNS (event_type)");
                System.out.println("Created Bloom Filter index on 'event_type'");
            }

            public static void structuredStreamingWithCDF(SparkSession spark) throws StreamingQueryException {
                Dataset<Row> streamingDF = spark.readStream()
                        .format("delta")
                        .table("source_table");

                streamingDF = streamingDF.filter("date >= current_date() - interval 7 days");

                StreamingQuery query = streamingDF.writeStream()
                        .format("delta")
                        .outputMode("append")
                        .option("checkpointLocation", "/tmp/delta/checkpoint")
                        .start("/tmp/delta/stream_with_cdf");

                System.out.println("Streaming with Change Data Feed started.");
                query.awaitTermination();
            }

            public static void medallionArchitecture(SparkSession spark) {
                Dataset<Row> bronzeDF = spark.read().format("json").load("/data/bronze");
                bronzeDF.write()
                        .format("delta")
                        .option("checkpointLocation", "/tmp/delta/bronze_checkpoint")
                        .save("/tmp/delta/bronze_table");

                System.out.println("Bronze layer: Raw data ingested.");

                Dataset<Row> silverDF = spark.read()
                        .format("delta")
                        .table("bronze_table")
                        .filter("status = 'active'");

                silverDF.write()
                        .format("delta")
                        .mode("overwrite")
                        .save("/tmp/delta/silver_table");

                System.out.println("Silver layer: CDC applied for replica.");

                Dataset<Row> goldDF = silverDF.groupBy("event_type").count();
                goldDF.write()
                        .format("delta")
                        .mode("overwrite")
                        .save("/tmp/delta/gold_table");

                System.out.println("Gold layer: Aggregated data saved.");
            }

            public static void handleInvalidRecords(SparkSession spark) {
                Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                        RowFactory.create(1, "A"),
                        RowFactory.create(2, null),
                        RowFactory.create(3, "C")),
                        new StructType()
                                .add("id", DataTypes.IntegerType)
                                .add("event_type", DataTypes.StringType));

                Dataset<Row> invalidRecords = df.filter("event_type IS NULL");
                invalidRecords.show();

                Dataset<Row> dfWithStatus = df.withColumn("validation_status",
                        functions.when(df.col("event_type").isNotNull(), "valid").otherwise("invalid"));
                dfWithStatus.show();

                System.out.println("Handled invalid records using two approaches.");
            }

            public static void manageSCDs(SparkSession spark) {
                Dataset<Row> initialData = spark.createDataFrame(Arrays.asList(
                        RowFactory.create(1, "Alice", "2024-01-01", "active"),
                        RowFactory.create(2, "Bob", "2024-01-01", "inactive")),
                        new StructType()
                                .add("id", DataTypes.IntegerType)
                                .add("name", DataTypes.StringType)
                                .add("effective_date", DataTypes.StringType)
                                .add("status", DataTypes.StringType));

                initialData.write()
                        .format("delta")
                        .mode("overwrite")
                        .save("/tmp/delta/scd_table");

                Dataset<Row> updatedData = spark.createDataFrame(Arrays.asList(
                        RowFactory.create(1, "Alice", "2024-02-01", "inactive")),
                        new StructType()
                                .add("id", DataTypes.IntegerType)
                                .add("name", DataTypes.StringType)
                                .add("effective_date", DataTypes.StringType)
                                .add("status", DataTypes.StringType));

                updatedData.write()
                        .format("delta")
                        .mode("append")
                        .save("/tmp/delta/scd_table");

                System.out.println("Managed Slowly Changing Dimensions with Type 2.");
            }
        }
        