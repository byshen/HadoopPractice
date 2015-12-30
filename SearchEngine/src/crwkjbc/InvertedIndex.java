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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.wltea.analyzer.core.Lexeme;
import org.wltea.analyzer.core.IKSegmenter;

import crwkjbc.*;
import java.util.Map; 


public class InvertedIndex
{
	public static class InvertedIndexerMapper  extends Mapper<LongWritable, Text, Text, SingleRecordWritable>
	{
		public static Text word = new Text();
		public static SingleRecordWritable singleRecord = new SingleRecordWritable();
		
		public void map(LongWritable key,Text val,Context context
		) throws IOException, InterruptedException 
		{
			String[] str=val.toString().split("\007", 7);
			HashMap<String, RankPosition> TokenMap=new HashMap<String, RankPosition>();
			System.out.println(str[0]);

			StringReader strreader_title=new StringReader(str[1]);
			IKSegmenter IK_title=new IKSegmenter(strreader_title, true);
			Lexeme lex=new Lexeme(0, 0, 0, 0);
			while((lex=IK_title.next())!=null)
			{
				if(lex.getLength()>=2)
				{
					String token = lex.getLexemeText();
					int start=lex.getBeginPosition();
					int end=lex.getEndPosition();
					if(!TokenMap.containsKey(token))
						TokenMap.put(token,new RankPosition(10,true,start,end));
					else
						TokenMap.put(token, TokenMap.get(token).add(10,true,start,end));
				}
			}
			
			StringReader strreader_place=new StringReader(str[2]);
			IKSegmenter IK_place=new IKSegmenter(strreader_place, true);
			Lexeme lex_place=new Lexeme(0, 0, 0, 0);
			while((lex_place=IK_place.next())!=null)
			{
				if(lex_place.getLength()>=2)
				{
					String token_place = lex_place.getLexemeText();
					int start=lex_place.getBeginPosition();
					int end=lex_place.getEndPosition();
					if(!TokenMap.containsKey(token_place))
						TokenMap.put(token_place,new RankPosition((float)(100.0/Float.valueOf(str[2].length())),false,start,end));
					else
						TokenMap.put(token_place,TokenMap.get(token_place).add((float)(500.0/Float.valueOf(str[2].length())),false,start,end));
				}
			}
			// requirements
			StringReader strreader_3=new StringReader(str[3]);
			IKSegmenter IK_3=new IKSegmenter(strreader_3, true);
			Lexeme lex_3=new Lexeme(0, 0, 0, 0);
			while((lex_3=IK_3.next())!=null)
			{
				if(lex_3.getLength()>=2)
				{
					String token_3 = lex_3.getLexemeText();
					int start=lex_3.getBeginPosition();
					int end=lex_3.getEndPosition();
					if(!TokenMap.containsKey(token_3))
						TokenMap.put(token_3,new RankPosition((float)(100.0/Float.valueOf(str[3].length())),false,start,end));
					else
						TokenMap.put(token_3,TokenMap.get(token_3).add((float)(500.0/Float.valueOf(str[3].length())),false,start,end));
				}
			}
			// xue li
			StringReader strreader_4=new StringReader(str[4]);
			IKSegmenter IK_4=new IKSegmenter(strreader_4, true);
			Lexeme lex_4=new Lexeme(0, 0, 0, 0);
			while((lex_4=IK_4.next())!=null)
			{
				if(lex_4.getLength()>=2)
				{
					String token_4 = lex_4.getLexemeText();
					int start=lex_4.getBeginPosition();
					int end=lex_4.getEndPosition();
					if(!TokenMap.containsKey(token_4))
						TokenMap.put(token_4,new RankPosition((float)(100.0/Float.valueOf(str[4].length())),false,start,end));
					else
						TokenMap.put(token_4,TokenMap.get(token_4).add((float)(500.0/Float.valueOf(str[4].length())),false,start,end));
				}
			}
			// 经验
			StringReader strreader_5=new StringReader(str[5]);
			IKSegmenter IK_5=new IKSegmenter(strreader_5, true);
			Lexeme lex_5=new Lexeme(0, 0, 0, 0);
			while((lex_5=IK_5.next())!=null)
			{
				if(lex_5.getLength()>=2)
				{
					String token_5 = lex_5.getLexemeText();
					int start=lex_5.getBeginPosition();
					int end=lex_5.getEndPosition();
					if(!TokenMap.containsKey(token_5))
						TokenMap.put(token_5,new RankPosition((float)(100.0/Float.valueOf(str[5].length())),false,start,end));
					else
						TokenMap.put(token_5,TokenMap.get(token_5).add((float)(500.0/Float.valueOf(str[5].length())),false,start,end));
				}
			}
			// 公司
			StringReader strreader_6=new StringReader(str[6]);
			IKSegmenter IK_6=new IKSegmenter(strreader_6, true);
			Lexeme lex_6=new Lexeme(0, 0, 0, 0);
			while((lex_6=IK_6.next())!=null)
			{
				if(lex_6.getLength()>=2)
				{
					String token_6 = lex_6.getLexemeText();
					int start=lex_6.getBeginPosition();
					int end=lex_6.getEndPosition();
					if(!TokenMap.containsKey(token_6))
						TokenMap.put(token_6,new RankPosition((float)(100.0/Float.valueOf(str[6].length())),false,start,end));
					else
						TokenMap.put(token_6,TokenMap.get(token_6).add((float)(600.0/Float.valueOf(str[6].length())),false,start,end));
				}
			}
			
			this.EmitMapValue(TokenMap,key,context);
			TokenMap.clear();
		}
		
		public void EmitMapValue(HashMap<String, RankPosition> TokenMap,LongWritable key,Context context) throws IOException, InterruptedException
		{
			for(Map.Entry<String,RankPosition> entry:TokenMap.entrySet())
			{
				word.set(entry.getKey());
				singleRecord.set(key.get(), entry.getValue());
				context.write(word, singleRecord);
			}
		}
	}
	//public static class InvertedIndexerCombiner extends 
	public static class InvertedIndexerReducer extends Reducer<Text,SingleRecordWritable,Text,Text> 
	{
		public void reduce(Text key, Iterable<SingleRecordWritable> values,Context context) throws IOException, InterruptedException 
		{
			boolean first = true;
			String toReturn="";
			for (SingleRecordWritable record : values)
			{
				if(!first)
					toReturn+=";";
				first=false;				
				toReturn+=record.GetDID().toString()+":"+record.GetRank().toString()+":"+record.GetPositions().toString();
			}
			Text out=new Text(toReturn);
			context.write(key,out);
			System.out.println(toReturn);
		}
	}
   
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException 
	{
		Configuration conf = new Configuration();
		String path0="/home/byshen/20151127";
		String path1="/home/byshen/output";
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Line Indexer");
		job.setInputFormatClass(TextInputFormat.class);
	    job.setJarByClass(InvertedIndex.class);
	    job.setMapperClass(InvertedIndexerMapper.class);
	    //job.setCombinerClass(InvertedIndexerReducer.class);
	    job.setReducerClass(InvertedIndexerReducer.class);
	    //job.setPartitionerClass(InvertedIndexerPartitioner.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(SingleRecordWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(path0));
	    FileOutputFormat.setOutputPath(job, new Path(path1));
	    job.setNumReduceTasks(16);
	    System.out.println("finished ....");
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}