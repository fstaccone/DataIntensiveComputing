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

    val topicSet=Set("dockless")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topicSet)
    
    val strings = messages.map(_._2)

    /*
    val filtered = strings.filter(x => x contains ",")
    val jsonStrings = filtered.map{ text =>
        val array: Option[IndexedSeq[String]] = Array.unapplySeq(text.split(","))
        var min = 0
        var hour = 0
        if (array.get(2).takeRight(2) == "00"){
            var min = 0
        }
        else{
            var min = array.get(2).takeRight(2)
        }
        if (array.get(2)(0) == "0"){
            var hour = 0
        }
        else{
            var hour = array.get(2).take(2)
        }
        val json: String = "{\"id\": " + array.get(0) + ", \"date\": \"" + array.get(1) + "\", \"hour\": " + hour + ", \"min\": " + min + ", \"location\": {\"lat\": " + array.get(3) + ", \"lon\": " + array.get(4) + "}, \"day_of_week\": " + array.get(5) + ", \"type\":\""+ array.get(6) + "\"}"
        json
    }
    */
      
    val jsonStrings = strings.filter(x => x contains ",").map{ text =>
        val array: Option[IndexedSeq[String]] = Array.unapplySeq(text.split(","))
        val json: String = "{\"id\": " + array.get(0) + ", \"date\": \"" + array.get(1) + "T"+ array.get(2) + "\", \"location\": {\"lat\": " + array.get(3) + ", \"lon\": " + array.get(4) + "}, \"type\":\""+ array.get(6) + "\"}"
        json
    }

    jsonStrings.window(Seconds(10), Seconds(10)).foreachRDD { rdd => rdd.saveJsonToEs("dockless/_doc") }

    ssc.start()
    ssc.awaitTermination()
  }
}