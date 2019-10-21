package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
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

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = 
        { ’class’ : ’SimpleStrategy’, ’replication_factor’ : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    // make a connection to Kafka and read (key, value) pairs from it - CONSUMER
    //<FILL IN>
    val kafkaConf = new SparkConf().setMaster("local[2]").setAppName("Ingesting Data from Kafka")

    val preferredHosts = LocationStrategies.PreferConsistent
    val topic = "avg"; // or Set("avg")
    val kafkaParams = Map(
        "metadata.broker.list" -> "localhost:9092",
        "zookeeper.connect" -> "localhost:2181",
        "group.id" -> "kafka-spark-streaming",
        "zookeeper.connection.timeout.ms" -> "1000")
    )

    /* OR
     "bootstrap.servers" -> "localhost:2181",
      "key.deserializer" -> classOf[StringDeserializer], 
      "value.deserializer" -> classOf[StringDeserializer], //MAYBE IntDese
      "group.id" -> "spark-streaming-notes",
      "auto.offset.reset" -> "earliest"
    */
    val messages = KafkaUtils.createDirectStream.[String, Int](
        ssc,
        preferredHosts,
        ConsumerStrategies.Subscribe[String, Int](topic, kafkaParams)
        )

    val value = messages.map{case (key, value) => value.split(',')} // get record
    val pairs = value.map{record => (record(1), record(2).toDouble)} // get pairs
    // GOT FROM https://jaceklaskowski.gitbooks.io/spark-streaming/spark-streaming-kafka-KafkaUtils.html

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[(Double, Int)]): (String, Double) = {
        val (sum, cnt) = state.getOption.getOrElse((0.0, 0))
        val newSum = value.getOrElse(0.0) + sum
        val newCnt = cnt + 1
        state.update((newSum, newCnt))
        (key, newSum/newCnt)
    }
    // https://stackoverflow.com/questions/52673546/how-to-pass-two-values-as-state-in-spark-streaming

    val spec = StateSpec.function(mappingFunc _)
    val stateDstream = pairs.mapWithState(spec)
    //val stateDstream = pairs.mapWithState(<FILL IN>)

    // store the result in Cassandra
    stateDstream.print() //  only?

    ssc.start()
    ssc.awaitTermination()
  }
}
