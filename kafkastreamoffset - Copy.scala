package org.inceptez.streaming
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import StreamingContext._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.json4s.{DefaultFormats,jackson}
object kafkastreamoffset {
def main(args:Array[String])
{
val sparkConf = new SparkConf().setAppName("kafkastream").setMaster("local[*]")
val sparkcontext = new SparkContext(sparkConf)
sparkcontext.setLogLevel("ERROR")
val ssc = new StreamingContext(sparkcontext, Seconds(200))
ssc.checkpoint("/tmp/checkpointdir1")
val kafkaParams = Map[String, Object](
"bootstrap.servers" -> "localhost:9092",
"key.deserializer" -> classOf[StringDeserializer],
"value.deserializer" -> classOf[StringDeserializer],
"group.id" -> "tk1",
"auto.offset.reset" -> "earliest"
)
val topics = Array("tk1")
val stream = KafkaUtils.createDirectStream[String, String](ssc,
PreferConsistent,
Subscribe[String, String](topics, kafkaParams)
)

//ssa 11.30 -> 11.45 
// 11.45 to 12 ()
// ssa 12.00

// 1-100 (11.45)
// 90 -100
// 101-200 (15 minutes)
// consumer tk1 100
// 101

stream.foreachRDD{
  a =>
    val offsetranges=a.asInstanceOf[HasOffsetRanges].offsetRanges
    val rdd1=a.flatMap(x => x.value().split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    rdd1.foreach(println)
rdd1.saveAsTextFile("hdfs://localhost:54310/user/hduser/kafkastreamout/outdir")
    stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetranges)//100
}
ssc.start()
ssc.awaitTermination()
}}




