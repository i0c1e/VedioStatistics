package com.victor.dao

import com.victor.domain.CategoryClickCount
import com.victor.utils.HBaseUtils
import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object CategoryClickCountDAO {
  //  val tableName = "category_clickcount"
  //  val cf = "info"
  //  val qualifer = "click_count"
  //
  //  def save(list: ListBuffer[CategoryClickCount]) = {
  //    val table = HBaseUtils.getInstance().getTable(tableName)
  //    var put: Put = null
  //    for (item <- list) {
  //      //      put = new Put(Bytes.toBytes(item.day_categoryID))
  //      //      put.addColumn(
  //      //        Bytes.toBytes(cf),
  //      //        Bytes.toBytes(qualifer),
  //      //        Bytes.toBytes(item.click_count)
  //      //      )
  //      //      table.put(put)
  //
  //
  //      table.incrementColumnValue(
  //        Bytes.toBytes(item.day_categoryID),
  //        Bytes.toBytes(cf),
  //        Bytes.toBytes(qualifer),
  //        item.click_count)
  //    }
  //  }
  //
  //  def count(day_category: String) = {
  //    val table = HBaseUtils.getInstance().getTable(tableName)
  //    val get = new Get(Bytes.toBytes(day_category))
  //    val value = table.get(get).getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifer))
  //    if (value == null) {
  //      0L
  //    } else {
  //      Bytes.toLong(value)
  //    }
  //  }
  //
  //  def main(args: Array[String]): Unit = {
  //    val list = new ListBuffer[CategoryClickCount]
  //    list.append(CategoryClickCount("20171122_1", 100))
  //    list.append(CategoryClickCount("20171122_2", 200))
  //    list.append(CategoryClickCount("20171122_3", 300))
  //    save(list)
  //    print(count("20171122_1"))
  //    print(count("20171122_2"))
  //    print(count("20171122_3"))
  //  }
  val tableName = "category_clickcount"
  val family = "info"
  val qualifer = "clickcount"

  def saveCount(list: ListBuffer[CategoryClickCount]) = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    for (item <- list) {
      table.incrementColumnValue(
        Bytes.toBytes(item.day_categoryID),
        Bytes.toBytes(family),
        Bytes.toBytes(qualifer),
        item.click_count
      )
    }
  }

  def getCount(day_categoryID: String) = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_categoryID))
    val value = table.get(get).getValue(Bytes.toBytes(family), Bytes.toBytes(qualifer))
    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    //    val list = new ListBuffer[CategoryClickCount]
    //    list.append(CategoryClickCount("20190314-1", 100))
    //    list.append(CategoryClickCount("20190314-2", 200))
    //    list.append(CategoryClickCount("20190314-3", 300))
    //    saveCount(list)
    val dance = getCount("20190114-dance")
    val digital = getCount("20190114-digital")
    val music = getCount("20190114-music")
    val game = getCount("20190114-game")
    val kichiku = getCount("20190114-kichiku")
    val technology = getCount("20190114-technology")

    println(s"dance:$dance\t\tdigital:$digital\t\tmusic:$music")
    println(s"game:$game\t\tkichiku:$kichiku\t\ttechnology:$technology")
  }
}
