package hg.hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class ExampleClient {
	public static void main(String[] args) throws IOException {
		
		  Configuration conf = HBaseConfiguration.create();
		  conf.set("hbase.zookeeper.quorum", "123.57.3.193");//使用eclipse时必须添加这个，否则无法定位
		  conf.set("hbase.zookeeper.property.clientPort", "2181");
		  HBaseAdmin admin = new HBaseAdmin(conf);// 新建一个数据库管理员
		  HTableDescriptor tableDescriptor = admin.getTableDescriptor(Bytes.toBytes("users"));
		  byte[] name = tableDescriptor.getName();
		  System.out.println("result:");

		  System.out.println("table name: "+ new String(name));
		  HColumnDescriptor[] columnFamilies = tableDescriptor
				  .getColumnFamilies();
		  for(HColumnDescriptor d : columnFamilies){
			  System.out.println("column Families: "+ d.getNameAsString());
			  }
	    }
}