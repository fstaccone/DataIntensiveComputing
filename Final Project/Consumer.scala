
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

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
import scala.util.parsing.json._
import scala.collection.mutable

object Consumer {
  def main(args: Array[String]) {

    // make a connection to Kafka and read (key, value) pairs from it
    val conf = new SparkConf().setAppName("csv").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    ssc.checkpoint("./checkpoint")

    val kafkaConf = Map(
        "metadata.broker.list" -> "localhost:9092",
        "zookeeper.connect" -> "localhost:2181",
        "group.id" -> "kafka-spark-streaming",
        "zookeeper.connection.timeout.ms" -> "1000")

    val topicSet=Set("test")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topicSet)
    
    val strings = messages.map(_._2)

    strings.print()

    //TODO: Create json starting from String messages
    val jsonString = strings.map{ text =>
        val array: Option[IndexedSeq[String]] = Array.unapplySeq(text.split(","))
        val json: String = "{\"id\":\"" + array.get(0) + "\", \"lat\":\"" + array.get(3) + "\", \"long\":\"" + array.get(4) + "\", \"type\":\""+ array.get(6) + "\"}"
        json
    }
        
    val storingLines = jsonString//JSON.parseRaw(jsonString).get.toString()

    storingLines.print()
/*
    import org.elasticsearch.spark.streaming._ 


    //TODO: Store json in elasticsearch
    val microbatches = mutable.Queue(storingLines)                

    // Save to Elasticsearch
    ssc.queueStream(microbatches).saveJsonToEs("dockless/doc") 

*/
    //TODO: Elasticsearch and Kibana: differnet versions! How to downgrade kibana from 7-4 to 6.8?

    //https://www.youtube.com/watch?v=5IiAZJSsz7I&t=88s how to use the visulization tools in kibana
    
    ssc.start()
    ssc.awaitTermination()
  }
}