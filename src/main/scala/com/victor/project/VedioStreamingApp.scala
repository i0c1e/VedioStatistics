package com.victor.project

import com.victor.dao.{CategoryClickCountDAO, CategorySearchClickCountDAO}
import com.victor.domain.{CategoryClickCount, CategorySearchClickCount, ClickLog}
import com.victor.project.Utils.TimeUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable.ListBuffer

object VedioStreamingApp {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[4]",
      "statStreamingApp", Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop131:9092,hadoop131:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("flumeTopic")
    val log = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    ).map(_.value())

    //    log.print()

    //132.124.143.167	2019-03-13 21:57:10	'GET www.bilibili.com/v/music HTTP/1.0'	https://www.baidu.com/s?wd=Fate-命运之夜	302

    //    val cleanLog = log.map(line => {
    //      var list = line.split("\t")
    //      var domain = list(2).split(" ")(1)
    //      var category: String = ""
    //      if (domain.startsWith("www.bilibili.com/v/")) {
    //        category = domain.split("/")(2)
    //      }
    //      ClickLog(list(0), TimeUtils.parseTime(list(1)), category, list(4), list(3))
    //    }
    //    ).filter(_.category != "")
    //    cleanLog.print()
    val cleanLog = log.map(line => {
      var list = line.split("\t")
      var domain = list(2).split(" ")(1)
      var category = ""
      if (domain.startsWith("www.bilibili.com/v/")) {
        category = domain.split("/")(2)
      }
      ClickLog(list(0), TimeUtils.parseTime(list(1)), category, list(4), list(3))
    }).filter(_.category != "")

    cleanLog.print()
    //每个类别每天的点击量
    cleanLog.map(log => {
      ((log.time.substring(0, 8) + "-" + log.category), 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(part => {
        val list = new ListBuffer[CategoryClickCount]
        part.foreach(per => {
          list.append(CategoryClickCount(per._1, per._2)
          )
        })
        CategoryClickCountDAO.saveCount(list)
      })
    })

    //统计来自不同网址的点击量
    //ClickLog(ip: String, time: String, category: String, statusCode: String,
    //                    referer: String)
    //https://www.baidu.com/s?wd=Fate-命运之夜
    cleanLog.map(log => {
      val refer = log.referer.replace("//", "/")
      val fields = refer.split("/")
      var domain = ""
      if (fields.length > 2) {
        domain = fields(1).split("\\.")(1)
      }
      (log.time, domain, log.category)
    }).filter(_._2 != "").map(log => {
      ((log._1 + "-" + log._2 + "-" + log._3), 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(part => {
        part.foreach(perRDD => {
          val list = new ListBuffer[CategorySearchClickCount]
          list.append(CategorySearchClickCount(perRDD._1, perRDD._2))
          CategorySearchClickCountDAO.saveCount(list)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
