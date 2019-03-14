package com.victor.dao

import com.victor.domain.CategorySearchClickCount
import com.victor.project.Utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object CategorySearchClickCountDAO {
  val tableName = "category_search_clickcount"
  val family = "info"
  val qualifer = "clickcount"

  def saveCount(list: ListBuffer[CategorySearchClickCount]) = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    for (item <- list) {
      table.incrementColumnValue(
        Bytes.toBytes(item.day_search_category),
        Bytes.toBytes(family),
        Bytes.toBytes(qualifer),
        item.click_count)
    }
  }

  def getCount(rowkey:String) = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(rowkey))
    val value = table.get(get).getValue(Bytes.toBytes(family),Bytes.toBytes(qualifer))
    if(value == null){
      0L
    }else{
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
//    val list = new ListBuffer[CategorySearchClickCount]
//    list.append(CategorySearchClickCount("20190314-anime",200))
//    list.append(CategorySearchClickCount("20190314-music",300))
//    list.append(CategorySearchClickCount("20190314-sound",600))
//    saveCount(list)
    println(getCount("20190114-baidu-dance"))
    println(getCount("20190114-baidu-digital"))
    println(getCount("20190114-baidu-game"))
    println(getCount("20190114-baidu-kichiku"))
    println(getCount("20190114-baidu-music"))
    println(getCount("20190114-baidu-technology"))
  }
}
