import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object StreamingDeequWindows {

  // Define case class for metrics including window information
  case class MetricRecordWithWindow(
                                     batchId: Long,
                                     windowStart: String,
                                     windowEnd: String,
                                     timestamp: String,
                                     batchSize: Long,
                                     numChecks: Int,
                                     executionTimeMs: Long,
                                     checkingTimeMs: Long,
                                   )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Streaming Deequ Example with Windowing")
      .master("local[*]") // Use all available cores for local testing
      .getOrCreate()

    // Set log level to WARN to reduce verbosity
    spark.sparkContext.setLogLevel("OFF")

    // Import implicits for Encoder
    import spark.implicits._

    // Needed for .asJava
    import scala.collection.JavaConverters._

    // Define the schema
    val itemSchema = StructType(Array(
      StructField("id", StringType, nullable = true),
      StructField("productName", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("priority", StringType, nullable = true),
      StructField("numViews", LongType, nullable = true),
      StructField("eventTime", TimestampType, nullable = true)
    ))

    // Kafka configuration
    val kafkaBootstrapServers = "localhost:9092" // Replace with your Kafka servers
    val kafkaTopic = "test_topic" // Replace with your Kafka topic

    // Create an empty Dataset[MetricRecordWithWindow] to store metrics
    var metricsDF = spark.emptyDataset[MetricRecordWithWindow]
    metricsDF.cache()

    // Read streaming data from Kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "latest")
      .load()

    // Extract and parse the JSON messages
    val messagesDF = kafkaDF.selectExpr("CAST(value AS STRING) as json_str")

    val parsedMessagesDF = messagesDF
      .withColumn("jsonData", from_json(col("json_str"), itemSchema))
      .select("jsonData.*")
      // If eventTime is a StringType, parse it to TimestampType
      .withColumn("eventTime", to_timestamp(col("eventTime"), "yyyy-MM-dd HH:mm:ss.SSS")) // Use appropriate format if needed

    // Apply watermark and windowing
    val windowedStream = parsedMessagesDF
      .withWatermark("eventTime", "0 seconds") // Adjust watermark duration as needed
      .groupBy(window(col("eventTime"), windowDuration = "20 seconds", slideDuration = "10 seconds")) // Adjust window duration as needed
      .agg(collect_list(struct(parsedMessagesDF.columns.map(col): _*)).alias("records"))

    // Process each windowed batch and apply Deequ checks
    val query = windowedStream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        // Ensure the batch data is cached to prevent multiple computations
        batchDF.cache()

        if (!batchDF.isEmpty) {
          val startTime = System.currentTimeMillis()
          // Collect the windowed data
          val windowedData = batchDF.collect()

          // Process each window separately
          windowedData.foreach { row =>
            val window = row.getAs[Row]("window")
            val windowStart = window.getAs[Timestamp]("start")
            val windowEnd = window.getAs[Timestamp]("end")
            val records = row.getAs[Seq[Row]]("records")

            // Convert records back to DataFrame
            val windowDF = spark.createDataFrame(records.asJava, parsedMessagesDF.schema)
            //            windowDF.show()

            val batchSize = windowDF.count()
            //            println(s"Window [$windowStart - $windowEnd], Batch size: $batchSize")

            if (batchSize > 0) {
              // Record the start time
              val windowingFinishTime = System.currentTimeMillis()

              // Define the data quality checks
              val check = Check(CheckLevel.Error, "Integrity checks")
                .isComplete("id")
                .isUnique("id")
                .hasMax("numViews", _ < 90)
                .isContainedIn("priority", Array("high", "medium", "low"))
                .isNonNegative("numViews")

              val verificationResult: VerificationResult = VerificationSuite()
                .onData(windowDF)
                .addCheck(check)
                .run()

              // Record the end time
              val endTime = System.currentTimeMillis()

              val numFailedChecks = verificationResult.checkResults
                .flatMap { case (_, checkResult) => checkResult.constraintResults }
                .count {
                  _.status != ConstraintStatus.Success
                }

              // Calculate total execution time and time required only for data quality checks
              val executionTime = endTime - startTime
              val checkingTime = endTime - windowingFinishTime

              // Get the number of checks executed
              val numChecks = numFailedChecks // change according to the experiment

              // Get current timestamp as a string
              val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))

              // Create a MetricRecord
              val metricRecord = MetricRecordWithWindow(
                batchId,
                windowStart.toString,
                windowEnd.toString,
                timestamp,
                batchSize,
                numChecks,
                executionTime,
                checkingTime
              )

              // Convert MetricRecord to Dataset
              val metricDS = Seq(metricRecord).toDS()

              // Append to metrics Dataset
              metricsDF = metricsDF.union(metricDS)

              // Optionally write metrics to a CSV file after each batch
              metricsDF.coalesce(1) // Optional: write to a single file
                .write
                .mode("overwrite")
                .option("header", "true")
                .csv("data/metrics.csv")

              println(s"batch $batchId: $executionTime ms in total (${batchSize.toDouble / executionTime} elem/sec)")
              println(s"batch $batchId: $checkingTime ms for windowing (${checkingTime.toDouble / executionTime * 100}% of execution time)")
              println(s"batch $batchId: $batchSize elements")
              println
            }
          }
        }

        // Unpersist the batch data
        batchDF.unpersist()
        ()
      }
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}