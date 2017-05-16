package cn.itcast.hadoop.mr.dc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
  
public class DataClear {

	public static class DCMapper extends Mapper<LongWritable, Text, Text, Text>{

				
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] fields = line.split("@#@#");	
			String[] keyword = {"好评","赞","物流快","质量好","服务热情"};
			String s = fields[(fields.length)-1];
			int j = 0;
			for(int i=0;i < keyword.length;i++){
				if (s.indexOf(keyword[i])!=-1) {
					break;
				}else{
					j+=1;
				}
			}
			if(j==5){
				context.write(new Text(fields[fields.length-1]),new Text());
			}
			}
		}

	public static class DCReducer extends Reducer<Text, Text, Text, Text>{
		
		private MultipleOutputs<Text,Text> mos;		
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
		mos = new MultipleOutputs<Text,Text>(context);
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {		
			String line = key.toString();
			int bytes = line.length();
			if (bytes <= 10) {
				mos.write("MOSText",key,"","小于10"+"/");
			}
			else if (bytes <= 20) {
				mos.write("MOSText",key,"","10~20"+"/");
			}
			else {
				mos.write("MOSText",key,"","大于20"+"/");
			}
			
					
		}
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			mos.close();
		}		
	}		
	
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(DataClear.class);
		
		job.setMapperClass(DCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setReducerClass(DCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		MultipleOutputs.addNamedOutput(job,"MOSText",TextOutputFormat.class,Text.class,Text.class);
		job.waitForCompletion(true);
		
	}
	

}
