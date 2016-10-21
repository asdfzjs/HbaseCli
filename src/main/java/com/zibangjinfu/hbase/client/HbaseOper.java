package com.zibangjinfu.hbase.client;

import com.zibangjinfu.hbase.exception.ModelTransException;
import jdk.jfr.events.ExceptionThrownEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by daniel on 16-10-17.
 */
public class HbaseOper {

    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;


    //初始化链接
    private static void init(){
        configuration = HBaseConfiguration.create();

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //关闭连接
    private static  void close(){
        try {
            if(null != admin)
                admin.close();
            if(null != connection)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //建表
    public static void createTable(String tableNmae,String[] cols) throws IOException {

        init();
        TableName tableName = TableName.valueOf(tableNmae);

        if(admin.tableExists(tableName)){
            System.out.println("talbe is exists!");
        }else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for(String col:cols){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
        close();
    }

    //删表
    public static void deleteTable(String tableName) throws IOException {
        init();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        close();
    }

    //查看已有表
    public static void listTables() throws IOException {
        init();
        HTableDescriptor hTableDescriptors[] = admin.listTables();
        for(HTableDescriptor hTableDescriptor :hTableDescriptors){
            System.out.println(hTableDescriptor.getNameAsString());
        }
        close();
    }

    //插入数据
    public static void insterRow(String tableName,String rowkey,String colFamily,String col,String val) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);
        table.close();
        close();
    }

    //删除数据
    public static void deleRow(String tableName, HbaseModel modelInfo, Filter filter) throws IOException {
        Table table = null;
        try {
            init();
            table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = transModel2Delete(modelInfo);
            table.delete(delete);
        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            if(table != null) {
                table.close();
            }
            close();
        }
    }

    /**
     *
     * @param tableName 需要删除数据表名字
     * @param modelInfoList 需要删除数据的列信息
     * @throws IOException
     */
    public static void deleRow(String tableName,List<HbaseModel> modelInfoList) throws IOException {
        Table table = null;
        try {
            init();
            table = connection.getTable(TableName.valueOf(tableName));
            //批量删除
            List<Delete> deleteList = new ArrayList<Delete>();
            for(HbaseModel modelInfo: modelInfoList){
                deleteList.add(transModel2Delete(modelInfo));
            }
            table.delete(deleteList);
        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            if(table != null) {
                table.close();
            }
            close();
        }
    }



    //根据rowkey查找数据
    public static void getData(String tableName,String rowkey,String colFamily,String col)throws  IOException{
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        //获取指定列族数据
        //get.addFamily(Bytes.toBytes(colFamily));
        //获取指定列数据
        //get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        Result result = table.get(get);

        showCell(result);
        table.close();
        close();
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

    //批量查找数据
    public static void scanData(String tableName,String startRow,String stopRow)throws IOException{
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        //scan.setStartRow(Bytes.toBytes(startRow));
        //scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
//            showCell(result);
        }
        table.close();
        close();
    }


    @SuppressWarnings("Since15")
    private static Delete transModel2Delete(HbaseModel modelInfo){
        if( modelInfo.getRowkey() == null){
            throw new ModelTransException("删除信息必须要需要有RowKey信息： "+modelInfo.toString());
        }
        Delete delete = new Delete(Bytes.toBytes(modelInfo.getRowkey()));
        if (modelInfo.getColFamily() != null && modelInfo.getCol() == null) {
            delete.addFamily(Bytes.toBytes(modelInfo.getColFamily()));
        }
        //删除指定列
        if (modelInfo.getColFamily() != null && modelInfo.getCol() !=null) {
            delete.addColumn(Bytes.toBytes(modelInfo.getColFamily()), Bytes.toBytes(modelInfo.getCol()));
        }
        return delete;

    }
}
