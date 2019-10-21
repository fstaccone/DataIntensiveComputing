package generator

import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import kafka.producer.KeyedMessage
import scala.io.Source


object ScalaProducerExample extends App {
    def getLineFromFile: String = {
        // read line	
        val firstLine = {
            val inputFile = Source.fromFile(inputPath)
            val line = inputFile.bufferedReader.readLine
            inputFile.close
            line
        }
    }.mkString

    val events = 10000
    val topic = "data"
    val brokers = "localhost:9092"
    val inputPath = "dataset.csv"

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "ScalaProducerExample")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    while (true) {
        val data = new ProducerRecord[String, String](topic, null, getLineFromFile)
        producer.send(data)
        print(data + "\n")
    }

    producer.close()
}
