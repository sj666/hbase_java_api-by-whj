package hbase_java_api_whj;

import com.google.common.collect.HashBasedTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class hbase {
    public static Configuration getHbaseConfiguration(){
        Configuration configuration =HBaseConfiguration.create();
        configuration.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
        configuration.addResource(new Path("/etc/hadoop/conf/core-site.xml"));

        return configuration;
    }
     public static void main(String[] args) throws IOException {
    Configuration configuration = hbase.getHbaseConfiguration();
         Connection connection=null;
         Admin admin=null;

         connection=ConnectionFactory.createConnection(configuration);
         admin=connection.getAdmin();
         //创建表
         String tableName="gammers";
         HTableDescriptor hbaseTable=new HTableDescriptor(TableName.valueOf(tableName));
         hbaseTable.addFamily(new HColumnDescriptor("name"));
         hbaseTable.addFamily(new HColumnDescriptor("city"));
         hbaseTable.addFamily(new HColumnDescriptor("company"));

         admin.createTable(hbaseTable);
         //插入数据
         Table table=null;
         connection=ConnectionFactory.createConnection(configuration);
        table=connection.getTable(TableName.valueOf("gammers"));

        //创建数据
         String[][] gammer={
                 {"1","marcel","beijing","dell"},
                 {"2","Franklin","meiguo","Phoenix"},
                 {"3","Dwayne","shanxi"," Apache Omid "},
                 {"4","xiaofeng","beijing","law"},
                 {"5","laocui","hebei"," Apache Tephra "},
                 {"6","qigemingzizhennan","hebei","dell"},
         };
         for (int i = 0; i < gammer.length; i++) {
             Put put =new Put(Bytes.toBytes(gammer[i][0]));
             put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("name"),Bytes.toBytes(gammer[i][1]));
             put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("city"),Bytes.toBytes(gammer[i][2]));
             put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("company"),Bytes.toBytes(gammer[i][3]));
                table.put(put);


         }

         ResultScanner resultScanner=null;
        Scan scan=new Scan();
        scan.addColumn(Bytes.toBytes("name"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("name"), Bytes.toBytes("city"));
        scan.addColumn(Bytes.toBytes("name"), Bytes.toBytes("company"));
         resultScanner = table.getScanner(scan);

         for (Result result = resultScanner.next(); result !=null;result = resultScanner.next()){
             byte[]names=result.getValue(Bytes.toBytes("name"), Bytes.toBytes("name"));
             byte[]city=result.getValue(Bytes.toBytes("name"), Bytes.toBytes("city"));
             byte[]company=result.getValue(Bytes.toBytes("name"), Bytes.toBytes("company"));
             String name=Bytes.toString(names);
             String citys=Bytes.toString(city);
             String companys=Bytes.toString(company);
             System.out.println("name : " + name + " --- city: " + citys + " --- company : " +companys );

         }
         }
    }
