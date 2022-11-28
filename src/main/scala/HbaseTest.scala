import org.apache.commons.text.WordUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions.{call_udf, col, from_json, lower}
import org.apache.spark.sql.types.{DataTypes, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.hadoop.hbase.util.Bytes
import java.io.IOException

object HbaseTest {


  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("spark://tranvietcuong-Latitude-7490:7077").appName("HbaseTest").getOrCreate()
    import spark.implicits._
    val df = spark
    .readStream
    .format("rate")
    .option("numPartitions", 1)
    .option("rowsPerSecond", 1)
    .load()
    //df.show()
    val writer = new  ForeachWriter[Row]{
      private var table: Table = _

      private var connection: Connection = _

      override def open(partitionId: Long, epochId: Long): Boolean = {
        val conf = HBaseConfiguration.create
        try {
          connection = ConnectionFactory.createConnection(conf)
          table = connection.getTable(TableName.valueOf("phone-calls1"))
          true
        } catch {
          case e: IOException =>
            throw new RuntimeException(e)
        }
      }

      override def process(value: Row): Unit = {
        val p = new Put(Bytes.toBytes(String.valueOf(value.getTimestamp(0))))
        p.addColumn(Bytes.toBytes("phonedata"), Bytes.toBytes("value"), Bytes.toBytes(value.getInt(1)))
        try table.put(p)
        catch {
          case e: IOException =>
            throw new RuntimeException(e)
        }
        System.out.println("data inserted")
      }

      override def close(errorOrNull: Throwable): Unit = {
        try {
          table.close()
          connection.close()
        } catch {
          case e: IOException =>
            throw new RuntimeException(e)
        }
      }
    }
    val query = df.writeStream.foreach(writer).start()
//    val query = df.writeStream.format("console").outputMode("append")
//      .start()
    query.awaitTermination();
    spark.stop()
  }

}
