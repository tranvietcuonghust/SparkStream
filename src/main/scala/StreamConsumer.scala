
import org.apache.commons.text.WordUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.{call_udf, col, from_json, lower}
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.api.java.JavaInputDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.sql.types.{DataTypes, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import java.sql.Timestamp

object StreamConsumer {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("spark://tranvietcuong-Latitude-7490:7077").appName("SparkStream").getOrCreate()
    import spark.implicits._
    val schema = StructType(Array(StructField("callId",LongType, false),
                            StructField("callerName", StringType, false), StructField("calleeName", StringType, false),
                            StructField("callerPhone", StringType, false), StructField("calleePhone", StringType, false),
                            StructField("timeStamp", TimestampType, false), StructField("Duration", IntegerType, false)))
    spark.udf.register("capi", (x: String) => WordUtils.capitalizeFully(x), DataTypes.StringType)
    val df = spark
      .readStream
      .format("kafka")
      //.format("json")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "phone-calls")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss","false")
      .load().selectExpr("CAST(value AS STRING)")
      .as("data").select("data.*")
      .select(from_json(col("value"),schema).as("json"))
      .select("json.*")
      .select(col("callId"),
            call_udf("capi", lower(col("callerName"))).as("callerName"),
            col("callerPhone"),
            call_udf("capi", lower(col("calleeName"))).as("calleeName"),
            col("calleePhone"),
            col("timeStamp"),
            col("Duration"))
    val query = df.writeStream
      .foreach(new HbaseDataWriter()).start()
    query.awaitTermination();
    spark.stop()
  }
}
