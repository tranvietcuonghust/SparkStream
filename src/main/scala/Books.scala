import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, rank, row_number, window}
object Books {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("spark://tranvietcuong-Latitude-7490:7077").appName("Books").getOrCreate()
    import spark.implicits._

    val books = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("books.csv")
    books.show()

    var res=books.withColumn("row",rank().over(Window.partitionBy("genre").orderBy(col("quantity").desc))).filter(col("row")<=2).drop("row")
    res.show()
    spark.stop()
  }

}
