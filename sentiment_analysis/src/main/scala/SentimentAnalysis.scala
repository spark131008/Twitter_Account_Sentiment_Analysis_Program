import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.Row
import org.apache.log4j._
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import java.io.FileNotFoundException
import scala.collection.mutable.ListBuffer

object SentimentAnalysis{
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
       .appName("Sentiment Analysis")
       .master("local[4]")
       .getOrCreate()

    import spark.implicits._

    val tweetDF = spark
        .read
        .option("inferSchema", "true")
        .parquet("/datalake2/*").cache()

    val testDF = tweetDF.withColumn("text", $"text".cast(StringType)).select($"text")

    val textToArray = testDF.collectAsList.toArray

    var listOfAalysisResult = ListBuffer[String]()
    for(i <- textToArray){
          val analysisResult = sentimentAnalysis(i.toString, spark)
          listOfAalysisResult += analysisResult
    }

    val finalDF = listOfAalysisResult.toDF("Sentiment-Analysis")
    val finalDF2 = finalDF.groupBy($"Sentiment-Analysis").count()
    val finalDF3 = finalDF2.groupBy("Sentiment-Analysis", "count").agg(sum("count") as "Sum" ).withColumn("Percentage", $"count" / sum("Sum").over())
    val finalDF4 = finalDF3.select($"Sentiment-Analysis", $"Sum", round(($"Percentage" * 100), 2) as "Percentage(%)").orderBy(desc("Percentage(%)"))
    finalDF4.show(false)
    spark.stop()
  }
  def sentimentAnalysis(text: String, spark: SparkSession): String = {
        import spark.implicits._
        //Read a text file and convert it into Dataframe
        val sentimentLexicon = spark.read
            .option("delimiter", " ")
            .csv("/datalake/Subjectivity_Lexicon.txt")
            .toDF("type", "length", "word", "pos1", "stemmed1", "sentiment").cache()

        val lexiconDF = sentimentLexicon.select(split(col("word"), "=")(1).as("Word"), split(col("sentiment"), "=")(1).as("Sentiment"))
        val removeDuplicateDF = lexiconDF.dropDuplicates("Word")
        val textToWords = text.split(", ")

        if(textToWords.size <= 1){
            val result = removeDuplicateDF.filter($"Word" === (text)).select($"Sentiment").collect().map(_.getString(0)).mkString(" ")
            if(result.isEmpty){
                return "mixed"
            }else{
                return result
            }
        }else{
            var scoreCount = 0
            textToWords.foreach{ x =>
                val result = removeDuplicateDF.filter($"Word" === (x)).select($"Sentiment").collect().map(_.getString(0)).mkString(" ")
                if(result.equals("positive")) scoreCount += 1
                else if(result.equals("negative")) scoreCount -=1
                else scoreCount += 0 
            }
            if(scoreCount > 0) return "positive"
            else if(scoreCount < 0) return "negative"
            else return "mixed"
        }
    }
}