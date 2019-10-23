import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
import scala.util.parsing.json._
import scala.collection.mutable
import org.elasticsearch.spark._

object Consumer {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("csv").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    ssc.checkpoint("./checkpoint")

    val kafkaConf = Map(
        "metadata.broker.list" -> "localhost:9092",
        "zookeeper.connect" -> "localhost:2181",
        "group.id" -> "kafka-spark-streaming",
        "zookeeper.connection.timeout.ms" -> "1000")

    val topicSet=Set("docklessvehicles")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topicSet)
    
    val strings = messages.map(_._2)
      
    val jsonStrings = strings.filter(x => x.split(",").length==9).map{ text =>
        val array: Option[IndexedSeq[String]] = Array.unapplySeq(text.split(","))
        val json: String = "{\"id\": " + array.get(0) + ", \"date\": \"" + array.get(1) + "T"+ array.get(2) + "\", \"duration\": " + array.get(3) + ", \"location\": {\"lat\": " + array.get(4) + ", \"lon\": " + array.get(5) + "}, \"day\": " + array.get(6) + ", \"hour\": " + array.get(7) + ", \"type\":\""+ array.get(8) + "\"}"
        json
    }

    jsonStrings.window(Seconds(10), Seconds(10)).foreachRDD { rdd => rdd.saveJsonToEs("dockless/_doc") }

    ssc.start()
    ssc.awaitTermination()
  }
}