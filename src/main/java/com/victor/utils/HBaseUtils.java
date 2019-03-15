package com.victor.utils; /**
 * @author Charles
 * @create 2019/3/13
 * @since 1.0.0
 */

//package com.victor.project.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtils {
    HBaseAdmin admin = null;
    Configuration conf = null;

    private HBaseUtils() {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop131,hadoop132,hadoop133");
        conf.set("hbase.rootdir", "hdfs://hadoop131:9000/hbase");
        try {
            admin = new HBaseAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance;

    public static synchronized HBaseUtils getInstance() {
        if (instance == null) {
            instance = new HBaseUtils();
            return instance;
        }
        return instance;
    }

    public HTable getTable(String tableName) {
        HTable table = null;
        try {
            table = new HTable(conf, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public void put(String tableName, String rowkey,
                    String family, String column, String value) {
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        HBaseUtils instance = HBaseUtils.getInstance();
        instance.put("category_clickcount", "20190313_1",
                "info", "click", "100");

    }
}
