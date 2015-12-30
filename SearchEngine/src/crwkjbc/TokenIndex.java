package crwkjbc;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.wltea.analyzer.core.Lexeme;
import org.wltea.analyzer.core.IKSegmenter;

import crwkjbc.*;
import crwkjbc.InvertedIndex.InvertedIndexerMapper;
import crwkjbc.InvertedIndex.InvertedIndexerReducer;

import java.util.Map; 

public class TokenIndex {
	public static class TokenIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String val = value.toString();
			FileSplit split = (FileSplit)context.getInputSplit();
//			System.out.print(val.split("\t").length );
			//if (val.split("\t").length > 1) {
			String[] tmp = val.split("\t");
			System.out.println(tmp[0]);
			System.out.println(tmp[1]);
			Integer lw = (int)(key.get()) + tmp[1].length() + tmp[0].length() +1;
			System.out.println(lw);
			context.write(new Text(tmp[0]), new Text(split.getPath().toString() + "\t" + key.toString() + "\t" + lw.toString()));
			//}
		}
	}
	public static class TokenIndexReducer extends Reducer<Text, Text, Text,Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			boolean flag = true;
			for (Text record:values) {
				if (flag) {
					System.out.println("Wrong Key Value: " + key);
				}
				flag = true;
				context.write(key, record);
			}
		}
	}
	
	public static void main(String[]args) 
			throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String path0="/home/byshen/HadoopPractice/SearchEngine/20151127out/";
		String path1="/home/byshen/HadoopPractice/SearchEngine/20151127out2";
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Token Index");
		job.setInputFormatClass(TextInputFormat.class);
	    job.setJarByClass(TokenIndex.class);
	    job.setMapperClass(TokenIndexMapper.class);
	    //job.setCombinerClass(InvertedIndexerReducer.class);
	    job.setReducerClass(TokenIndexReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(1);
	    
	    FileInputFormat.addInputPath(job, new Path(path0));
	    FileOutputFormat.setOutputPath(job, new Path(path1));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
