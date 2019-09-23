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
    val cluster = Cluster.<FILL IN>
    val session = cluster.connect()
    session.execute(.<FILL IN>)

    // make a connection to Kafka and read (key, value) pairs from it
    <FILL IN>
    val kafkaConf = <FILL IN>
    val messages = KafkaUtils.createDirectStream.<FILL IN>
    <FILL IN>

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
	<FILL IN>
    }
    val stateDstream = pairs.mapWithState(<FILL IN>)

    // store the result in Cassandra
    stateDstream.<FILL IN>

    ssc.start()
    ssc.awaitTermination()
  }
}
