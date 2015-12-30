import java.io.IOException;  
import java.util.HashMap;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.client.Result;  
import org.apache.hadoop.hbase.client.Scan;  
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;  
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;  
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;  
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;  
import org.apache.hadoop.hbase.util.Bytes;  
import org.apache.hadoop.io.Writable;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.util.GenericOptionsParser;  

public class IndexBuilder {
	// 索引的列族为info ，列为name
	public static final byte[] column = Bytes.toBytes("info");
	public static final byte[] qualifier = Bytes.toBytes("name");
	
	
	public static class Map extends Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Writable> {
		private byte[] family;
		// indexes 存储了列与表对对应关系，其中byte［］用于获取列的值作为索引表的键值，ImmutableBytesWritable作为表的名称
		private HashMap<byte[], ImmutableBytesWritable> indexes;
		// 在Map中，对每一行的数据提取出需要建立索引的列的值，加入到索引表中输出。
		protected void map(ImmutableBytesWritable rowKey, Result result,Context context) throws IOException, InterruptedException {
			// original: row 123 attribute:phone 555-1212
	        // index: row 555-1212 INDEX:ROW 123
			for (java.util.Map.Entry<byte[], ImmutableBytesWritable> index : indexes.entrySet()) {
				// 获得列名 qualifier
				byte[] qualifier = index.getKey();
				// 索引表名 table
				ImmutableBytesWritable table = index.getValue();
				// 插入的列值为 列名加上行名
				byte[] value = result.getValue(family, qualifier);
				// 以列值作为行键，在列“info：row”中插入行键
				Put put = new Put(value);
				put.add(column, qualifier, rowKey.get());
				// 在table表上执行put操作
				context.write(table, (Writable) put);
			}
		}


		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration configuration = context.getConfiguration();
			String table = configuration.get("index.tablename");
			String[] fields = configuration.getStrings("index.fields");
			String familyName = configuration.get("index.familyname");
			family = Bytes.toBytes(familyName);

			// 初始化indexes   
			// if the table is "people" and the field to index is "email", then the
	        // index table will be called "people-email"
			indexes = new HashMap<byte[], ImmutableBytesWritable>();
			for (String field : fields) {
				indexes.put(Bytes.toBytes(field), new ImmutableBytesWritable(
						Bytes.toBytes(table + "-" + field)));
			}
		}
	}
	// Job configuration
	public static Job configureJob(Configuration conf, String[] args)throws IOException {
		String table = args[0];
		String columnFamily = args[1];
		System.out.println(table);
		// 通过Configuration.set()方法传递参数
		conf.set(TableInputFormat.SCAN, ScanToString(new Scan()));
		conf.set(TableInputFormat.INPUT_TABLE, table);
		conf.set("index.tablename", table);
		conf.set("index.familyname", columnFamily);
		String[] fields = new String[args.length - 2];
		for (int i = 0; i < fields.length; i++) {
			fields[i] = args[i + 2];
		}
		conf.setStrings("index.fields", fields);
		conf.set("index.familyname", "attributes");
		// 运行参数配置
		Job job = new Job(conf, table);
		job.setJarByClass(IndexBuilder.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TableInputFormat.class);
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		return job;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3)System.exit(-1);
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	static String ScanToString(Scan scan) throws IOException {
	    ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
	    return Base64.encodeBytes(proto.toByteArray());
	}
}