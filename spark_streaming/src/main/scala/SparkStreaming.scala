import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.Row
import org.apache.log4j._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.StringType

object SparkStreaming {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
       .appName("spark_streaming")
       .master("local[4]")
       .getOrCreate()

    import spark.implicits._

    val schemaDF = spark.read.json("/datalake1/tweetstream-1")
    val streamDF = spark
        .readStream
        .schema(schemaDF.schema)
        .json("/datalake1")

    val tweetDF = streamDF.select(split(col("data.text"), "\\W+").as("text"))
    
    val block_sz = 1024
    val streamQuery = tweetDF
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("parquet.block.size", block_sz)
        .option("path", "/datalake2")
        .option("checkpointLocation", "/checkpoint_dir")
        .start()

    streamQuery.awaitTermination()
  }
}