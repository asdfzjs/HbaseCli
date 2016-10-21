package com.zibangjinfu.hbase.client;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.LoggerFactory;


public class HbaseCli {
	private static final long serialVersionUID = 1L;
	/**
	 * 管理hbase表
	 */
	private static HBaseAdmin admin;

	private List<Row> logInfoBatch = new ArrayList<Row>();

	private List<Tuple> logInfoTuple = new ArrayList<Tuple>();


	private OutputCollector collector;


	/**
	 * 对表中的数据CRDU的对象
	 */
	private static HTable htable;
	static {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "node1,node2,node3,node4,node5");
		try {
			admin = new HBaseAdmin(config);
			htable = new HTable(config, "ywlog");
			htable.setWriteBufferSize(5 * 1024 * 1024); // 5MB
			htable.setAutoFlush(false);
		} catch (IOException e) {
			e.printStackTrace();
		} 
    }  
 
	public static void main(String[] args) throws IOException {
		Get get = new Get("fdc111b0-122b-498f-88ab-148f91be5ef0_20161017213807_3_94".getBytes());
		Result result = htable.get(get);
		showCell(result);
//		Put put =  new  Put(Bytes.toBytes ("fdc111b0-122b-498f-88ab-148f91be5ef0_20161017213807_3_94"));
//	    put.add(Bytes.toBytes ("log"), Bytes.toBytes ( "name" ), Bytes.toBytes ("lee" ));
//	    put.add(Bytes.toBytes ( "log" ), Bytes.toBytes ( "address" ), Bytes.toBytes ("longze" ));
//	    put.add(Bytes.toBytes ( "log" ), Bytes.toBytes ( "age" ), Bytes.toBytes ("31" ));
//	    put.setDurability(Durability. SYNC_WAL );
//	    htable.put(put);         
//	    htable.close();
	}
	//格式化输出
    public static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
            System.out.println("Timetamp:"+cell.getTimestamp()+" ");
            System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
        }
    }
	
	
}

