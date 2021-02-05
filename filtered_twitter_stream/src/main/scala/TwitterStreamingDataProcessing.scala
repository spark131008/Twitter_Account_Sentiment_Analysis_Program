import org.apache.http.impl.client.HttpClients
import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.util.EntityUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.nio.file.Paths
import java.nio.file.Files
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._
import org.json.JSONArray
import org.json.JSONObject


object TwitterStreamingDataProcessing {

  def main(args: Array[String]): Unit = {
    var bearerToken: String = "AAAAAAAAAAAAAAAAAAAAABwjLwEAAAAAmKwfSHgPu1hd0BLNHyMAJW%2FlYRM%3DVONcD8xJw8Jg2z41n9wnU4Ea0GqG798AkmkoWKtiN2RnJIkApZ"

    if (bearerToken != null) {
      var rules = Map[String, String]()
      rules("from:McDonalds") =  "McDonalds account"

      setupRules(bearerToken, rules)
      tweetStreamToDir(bearerToken)
    } else {
       println("There was a problem getting you bearer token. Please make sure you set the BEARER_TOKEN environment variable")
    }
  }


  def tweetStreamToDir(bearerToken: String) {
      val httpClient = HttpClients.custom.setDefaultRequestConfig(
          RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build
      ).build

      //val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/sample/stream")
      val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream")


      val httpGet = new HttpGet(uriBuilder.build)
      httpGet.setHeader("Authorization", s"Bearer ${bearerToken}")

      val response = httpClient.execute(httpGet)
      val entity = response.getEntity()
      if(entity != null){
        val reader = new BufferedReader(new InputStreamReader(entity.getContent))
        var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
        var (lineNumber, line) = (1, reader.readLine())
        val (linesPerFile, milliseconds) = (10, System.currentTimeMillis())
        while (line != null) {
          if (lineNumber % linesPerFile == 0) {
            fileWriter.close()
            Files.move(
            Paths.get("tweetstream.tmp"),
            Paths.get(s"/datalake1/tweetstream-${lineNumber/linesPerFile}")
            )
            fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
          }
          fileWriter.println(line)
          line = reader.readLine()
          println(line)
          lineNumber += 1
      }
    }
  }

  def setupRules(bearerToken: String, rules: Map[String, String]) {
    //var existingRules = getRules(bearerToken)
    // if (existingRules.size > 0) {
    //   deleteRules(bearerToken, existingRules)
    // }

    // var existingRules = List("")
    // deleteRules(bearerToken, existingRules)
    createRules(bearerToken, rules)
  }

  def getRules(bearerToken: String): List[String] = {
    var rules = ArrayBuffer[String]()
    val httpClient = HttpClients.custom.setDefaultRequestConfig(
      RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build
    ).build

    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", s"Bearer ${bearerToken}")
    httpGet.setHeader("content-type", "application/json")

    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()

    if (entity != null) {
      val reader = new BufferedReader(new InputStreamReader(entity.getContent))
      val line = reader.readLine()
      println(line)
      // if (json.length > 1) {
      //   var array = new JSONArray(json.get("data"))
      //   for (i <- 0 until array.length) {
      //     var jsonObject = array.getJSONObject(i)
      //     rules += jsonObject.getString("id")
      //   }
      // }
    }
    return rules.toList
  }

  def deleteRules(bearerToken: String, existingRules: List[String]) {
    val httpClient = HttpClients.custom.setDefaultRequestConfig(
      RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build
    ).build

    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules")

    val httpPost = new HttpPost(uriBuilder.build)
    httpPost.setHeader("Authorization", s"Bearer ${bearerToken}")
    httpPost.setHeader("content-type", "application/json")

    val body = new StringEntity(getFormattedString(s"{ ${'"'}delete${'"'}: { ${'"'}ids${'"'}: [%s]}}", existingRules))
    httpPost.setEntity(body)

    val response = httpClient.execute(httpPost)
    val entity = response.getEntity()
    if (entity != null) {
      println(EntityUtils.toString(entity, "UTF-8"));
    }
  }

  def createRules(bearerToken: String, rules: Map[String, String]) {
    val httpClient = HttpClients.custom.setDefaultRequestConfig(
      RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build
    ).build

    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules")

    val httpPost = new HttpPost(uriBuilder.build)
    httpPost.setHeader("Authorization", s"Bearer ${bearerToken}")
    httpPost.setHeader("content-type", "application/json")

    val body = new StringEntity(getFormattedString(s"{${'"'}add${'"'}: [%s]}", rules))
    httpPost.setEntity(body)
    val response = httpClient.execute(httpPost)
    val entity = response.getEntity()
    if (entity != null) {
      println(EntityUtils.toString(entity, "UTF-8"));
    }
  }

  def getFormattedString(string: String, ids: List[String]): String = {
    var sb = new StringBuilder()
    if (ids.size == 1) {
      return string.format(s"${'"'}" + ids(0) + s"${'"'}")
    } 
    else {
      for (id <- ids) {
        sb.append(s"${'"'}" + id + s"${'"'}" + ",")
      }
      val result = sb.toString()
      return string.format(result.substring(0, result.length - 1))
    }
  }

  def getFormattedString(string: String, rules: Map[String, String]): String = {
    var sb = new StringBuilder()
    if (rules.size == 1) {
      rules.keys.foreach{ (key) =>
        sb.append(s"{${'"'}value${'"'}: ${'"'}" + key + s"${'"'}, ${'"'}tag${'"'}: ${'"'}" + rules(key) + s"${'"'}}")
      }
      val result = sb.toString()
      return string.format(result.substring(0))
    } 
    else {
      rules.keys.foreach{ (key) =>
        sb.append(s"{${'"'}value${'"'}: ${'"'}" + key + s"${'"'}, ${'"'}tag${'"'}: ${'"'}" + rules(key) + s"${'"'}}" + ",")
      }
      val result = sb.toString()
      return string.format(result.substring(0, result.length - 1))
    }
  }
}
