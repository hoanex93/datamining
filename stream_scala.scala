package testerproda

import java.nio.charset.Charset

import org.apache.commons.codec.binary.Base64
import org.apache.http.client.methods._
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

//mport org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

//import testerproda.CustomReceiver

object streamstock {
  //format data
  case class Obs(movie: Double, love: Double, delicious: Double, thinking: Double, location: Double, gender: Double)

  //transform data
  def parseObs(line: Array[Double]): Obs = {
    Obs(
      line(0), line(1), line(2), line(3), line(4), line(5)
    )
  }

  //clean data
  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    rdd.map(_.split(","))
      .map(_.map(_.toDouble))
  }

  def main(args: Array[String]): Unit = {

    //create a session
    val sparkobj = SparkSession
      .builder()
      .appName("normstream")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate

    //set new runtime options
    sparkobj.conf.set("spark.sql.shuffle.partitions", 6)
    sparkobj.conf.set("spark.executor.memory", "2g")

    //get all settings
    // val configMap:Map[String, String] = sparkobj.conf.getAll()

    val sc = sparkobj.sparkContext

    val urlvar = "https://internal.quoine.com/products/50/price_levels"
    val request = new HttpGet(urlvar)

    val ssc = new StreamingContext(sc, Seconds(10))


    // load the data into a DataFrame
    //val linesDStream = ssc.socketTextStream("https://internal.quoine.com/products/50/price_levels",80)
    //val  lines = ssc.socketTextStream(urlvar,80)

    //val customReceiverStream = ssc.receiverStream(new CustomReceiver(host, port))
    //val words = customReceiverStream.flatMap(_.split(" "))



    // val flumeStream = FlumeUtils.createStream(ssc, "https://internal.quoine.com/products/50/price_levels", 80)
    //val rdd = sc.textFile("data/usernorm.csv")
    //val objrdd = parseRDD(rdd).map(parseObs)
    // val obsdf = objrdd.toDF().cache()

    //linesDStream.saveAsTextFiles("/flume_Map_", "_Mapout")
    //val words = linesDStream.flatMap(_.split(" "))
    //ssc.start()
    //ssc.awaitTermination()

    //val words = lines.flatMap(_.split(" "))

    //val pairs = words.map(word => (word, 1))
    //val wordCounts = pairs.reduceByKey(_ + _)


    println(s"STREAM-----------> ${getRestContent(request)}")
  }

  //get API json result
  private def getRestContent(request: HttpUriRequest): String = {
    val user = ""
    val password = ""
    val base64Auth = Base64.encodeBase64String(s"$user:$password".getBytes(Charset.forName("UTF-8")))

    request.setHeader("Authorization", s"Basic $base64Auth")
    request.setHeader("Content-Type", "application/json")
    val httpClient = new DefaultHttpClient()
    val httpResponse = httpClient.execute(request)
    val content = Option(httpResponse.getEntity()).map { entity =>
      val inputStream = entity.getContent()
      try {
        Source.fromInputStream(inputStream).getLines.mkString
      } finally {
        inputStream.close()
        httpClient.getConnectionManager().shutdown()
      }
    }.getOrElse("")
    return content
  }
}
