package sparkstreaming

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
import java.sql.Timestamp

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {

    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count double);")

    // make a connection to Kafka and read (key, value) pairs from it
    val conf = new SparkConf().setAppName("avg").setMaster("local[2]")
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    ssc.checkpoint("./checkpoint")

    val kafkaConf = Map(
        "metadata.broker.list" -> "localhost:9092",
        "zookeeper.connect" -> "localhost:2181",
        "group.id" -> "kafka-spark-streaming",
        "zookeeper.connection.timeout.ms" -> "1000")

    val topicSet=Set("avg")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topicSet)
    



    // ---------------- end of Staccon's part
    val values = messages.map{case (key, value) => value.split(',')}    
    val pairs = values.map(record => (record(0), record(1).toDouble))

    // measure the average value for each key in a stateful manner
    
    def mappingFunc(key: String, value: Option[Double], state: State[(Double, Int)]): (String, Double) = {
      var prevState: (Double, Int) = state.getOption.getOrElse[(Double, Int)]((0.0, 0))
      val sum = prevState._1 * prevState._2
      val count = prevState._2 + 1
      val newState: (Double, Int) = ((sum + value.get) / count, count)
      state.update(newState)  
      val output = (key, state.getOption.get._1)
      output
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))





    // ------------------------------------------

    //OPTION 1: make structured data, and then save them in noSQL MapR-DB

    // Create class structuring the data received
    case class Dockless(id: String, tm: java.sql.Timestamp, lat: Double, lon:Double,
      wday: Integer, type: Integer) extends Serializable


    // Given the string as csv, return the structured object
    def parseDockless(str: String): Dockless = {
      val p = str.split(",")

      //         id    tm    lat             long                wday         type
      Dockless(p(0), p(1), p(2).toDouble), p(3).toDouble), p(4).toInteger, p(5).toDouble) 
    }

    // store the result in MapR database

    /*
    val spark = SparkSession
      .builder
      .appName("StructuredKafkaWordCount")
      .getOrCreate()
    */
    //GET SESSION = spark
              
    import spark.implicits._

    // In the code below, we register a user-defined function (UDF) to deserialize 
    // the message value strings using the parseDockless function.Then we use the UDF 
    // in a select expression with a String Cast of the df1 column value, which 
    // returns a DataFrame of Dockless objects.

    // https://mapr.com/blog/real-time-analysis-popular-uber-locations-spark-structured-streaming-machine-learning-kafka-and-mapr-db/

    val sdb: Dataset[Dockless] = clusters.map(message => parseDockless(message))

    val query = sdb
      .writeStream
      .queryName("dockless")
      .format("memory")
      .outputMode("append")
      .start


    query.awaitTermination()


    // OPTION 2: receive data as string, split and store not-structured
                  
    //Generate separate fields for columns and store in MapR-DB
    val storingLines = messages.flatMap(_.split(","))
                  
    //Run the query that saves the result to MapR-DB
    val query = storingLines.writeStream
       .outputMode("complete")
       .format("console")
       .option("checkpointLocation", checkpointLocation)
       .start()
                  
    query.awaitTermination()


  // ------------------------------------------





    ssc.start()
    ssc.awaitTermination()
  }

}
