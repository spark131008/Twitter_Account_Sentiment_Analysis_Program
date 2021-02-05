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

    val newValueDF = tweetDF.withColumn("newValue", $"text".cast(StringType)).select($"newValue")
    val textToArray = newValueDF.collectAsList.toArray
    var mappingTextAndAnalysis = ListBuffer[Map[String, String]]()
    for(i <- textToArray){
          val analysisResult = sentimentAnalysis(i.toString, spark)
          mappingTextAndAnalysis += Map(i.toString -> analysisResult)
    }

    val sentimentResultToList = mappingTextAndAnalysis.toDF()
    val finalDF = sentimentResultToList.select((map_keys($"value"))(0) as "Text", (map_values($"value"))(0) as "Sentiment-Analysis")

    val finalDF2 = finalDF.groupBy("Sentiment-Analysis").count()
    val finalDF3 = finalDF2.groupBy("Sentiment-Analysis", "count" ).agg(sum("count") as "Sum" ) .withColumn("Percentage", $"count" / sum("Sum").over())
    val finalDF4 = finalDF3.select($"Sentiment-Analysis", $"Sum", round(($"Percentage" * 100), 2) as "Percentage(%)").orderBy(desc("Sentiment-Analysis")).toDF()
    finalDF4.show(false)

  }
  def sentimentAnalysis(text: String, spark: SparkSession): String = {
        import spark.implicits._
        //Read a text file and convert it into Dataframe
        val sentimentLexicon = spark.read
            .option("inferSchema", "true")
            .textFile("/datalake/Subjectivity_Lexicon.txt").cache()

        val lexiconToArray = sentimentLexicon.collectAsList.toArray
        var mappingWordAndSentiment = ListBuffer[Map[String, String]]()
        for(i <- lexiconToArray){
            val words = i.toString.split(" ")
            var word1 = ""
            var sentiment1 = ""
            for(j <- words){
                if(j.contains("word1")) {
                    word1 = j.split("=")(1)
                }else if(j.contains("priorpolarity")) {
                    sentiment1 = j.split("=")(1)
                }
            }
            mappingWordAndSentiment += Map(word1 -> sentiment1)
        }

        val lexiconToList = mappingWordAndSentiment.toDF()
        val finalDF = lexiconToList.select((map_keys($"value"))(0) as "Word", (map_values($"value"))(0) as "Sentiment")
        val removeDuplicateDF = finalDF.dropDuplicates("Word")
        val textToWords = text.split(" ")

        if(textToWords.size <= 1){
            val filterDF = removeDuplicateDF.filter($"Word" === (text))
            val result = filterDF.select($"Sentiment").collect().map(_.getString(0)).mkString(" ")
            if(result.isEmpty){
                return "mixed"
            }else{
                return result
            }
        }else{
            var scoreCount = 0
            textToWords.foreach{ x =>
                val filterDF = removeDuplicateDF.filter($"Word" === (x))
                val result = filterDF.select($"Sentiment").collect().map(_.getString(0)).mkString(" ")
                if(result.equals("positive")) scoreCount += 1
                else if(result.equals("negative")) scoreCount -=1
                else if(result.isEmpty) scoreCount += 0 
            }
            if(scoreCount > 0) return "positive"
            else if(scoreCount < 0) return "negative"
            else return "mixed"
        }
    }
}