package lewis;

import net.sourceforge.sizeof.SizeOf;

import org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler;
import java.text.DecimalFormat;
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import btree.*;



public class BtreeForHive {

	static Configuration conf = new Configuration();
	static FileSystem local;
	static FileSystem hdfs;

	static{
		conf.set("fs.default.name", "hdfs://localhost:9000");
		try{
			hdfs = FileSystem.get(conf);
			local = FileSystem.getLocal(conf);
		}catch(IOException error){
			error.printStackTrace();
		}
	}
	
	/*
	 * 提取unew来进行区分
	 */
	public static class map1 extends Mapper<Object, Text, IntWritable, Text>{
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String tmp = value.toString();
			String[] word = tmp.split("\t");
			if(word.length != 4) return;
			
			IntWritable unum = new IntWritable(Integer.valueOf(word[2]));
			context.write(unum, value);
		}
	}
	
	/*
	 * partitioner用以将unum按照顺序分配到特定的reducer上面.
	 */
	public static class par extends Partitioner<IntWritable, Text>{
		int range = 1000; // unum 的范围
		public int getPartition(IntWritable key, Text value, int numReduceTasks){
			int unum = key.get();
			double a = (unum+0.0)/1000;
			return (int)(a*numReduceTasks);
		}
	}
	
	public static class red1 extends Reducer<IntWritable, Text, IntWritable, Text>{
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text i:values){
				context.write(key, i);
			}
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, IllegalArgumentException, IllegalAccessException {
		// TODO Auto-generated method stub
		
		/*
		 * job1 使用partition进行分区表单的建立.
		 * 相当于把总数据表分区为子表,表单之间整体有序
		 */
		long time1;long time2 ;
		
		Path job1Input = new Path("/user/hive/warehouse/hive2.db/nonsuit");
		Path job1Output = new Path("/user/lewis/partitionTable");
		if(hdfs.exists(job1Output)){
			hdfs.delete(job1Output);
		}
		
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(BtreeForHive.class);
		
		job1.setMapperClass(map1.class);
		job1.setReducerClass(red1.class);
		job1.setPartitionerClass(par.class);
		
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);			//  设定输出的key为Text
		job1.setOutputValueClass(Text.class);		//  设定输出的value为Text(文件名:次数)
		
		job1.setNumReduceTasks(5);
		FileInputFormat.addInputPath(job1, job1Input);	// 加入文件输入路径
		FileOutputFormat.setOutputPath(job1, job1Output);	// 加入文件输出路径
		
		time1 = System.currentTimeMillis();
		job1.waitForCompletion(true);
		time2 = System.currentTimeMillis();
		
		System.out.println("job1 is over. Time: " + (time2-time1)/1000);
		
		
		/*
		 * 建立索引,输出到output上面
		 */
		/*
		Path job2Input = new Path("/user/lewis/Input");
		if(hdfs.exists(job2Input)){
			hdfs.delete(job2Input);
			hdfs.mkdirs(job2Input);
		}
		FSDataOutputStream output = null;
		hdfs.create(new Path("/user/lewis/Input/input.txt"));
		for(int i=0; i < 5; ++i){
			
		}
		*/
		
		/*
		 *  查询表单
		 */
		
		double a = Integer.valueOf(args[0])/1000.0*5;
		String bb = String.valueOf((int)a);
		FSDataInputStream fs = hdfs.open(new Path("/user/lewis/partitionTable/part-r-0000"+bb));  
        	BufferedReader bis = new BufferedReader(new InputStreamReader(fs));
       		System.out.println(bb);
        
        /*
         * 10叉树
         */
        time1 = System.currentTimeMillis();
        BplusTree tree = new BplusTree(10);
		String tmp;
		Record r;
		int LastNum = 0;
		block b = new block();
		while((tmp=bis.readLine())!=null){
			String[] Context = tmp.split("\t");
			if(Context.length != 5)continue;
			if(Integer.valueOf(Context[0])!=LastNum && !b.isEmpty()){
				
				tree.insertOrUpdate(Integer.valueOf(Context[0]), b);
				b = new block();
				LastNum = Integer.valueOf(Context[0]);
			}
			
			r = new Record(Arrays.copyOfRange(Context, 1, 5));
			b.add(r);
		}
		/*
		 * 查询测试
		 */
		block aa = (block)tree.get(Integer.valueOf(args[0]));
		if(!aa.isEmpty()){
			aa.listShow();
		}
		else{
			System.out.println("no Result.");
		}
		time2=System.currentTimeMillis();
		System.out.println("Test Time: "+ (time2-time1));
	}

	
}
