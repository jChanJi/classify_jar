package cn.itcast.hadoop.mr.dc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataClassify {

	public static class DCMapper extends Mapper<LongWritable, Text, Text, Text>{

		ArrayList<String> brandList = null;
		private String brandFilePath ;
		
		public void readBrand(Context context) throws IOException{  
	        if(brandList == null){  
	            brandList = new ArrayList<String>();  
	        }else if(!brandList.isEmpty()){  
	            brandList.clear();  
	        }  
	        FileSystem fs = FileSystem.get(context.getConfiguration());  		
		
	        // 读取文件列表  
	        Path filePath =new Path(brandFilePath);   	        
//	        FileStatus stats[] = fs.listStatus(filePath);  
	        String s = "";  	        
           Path inFile =new Path(filePath.toString());  
            FSDataInputStream fin = fs.open(inFile);  
            BufferedReader input = new BufferedReader(new InputStreamReader(fin, "UTF-8"));  	 
            
            // 处理当前文件数据  
            while ((s = input.readLine()) != null) {  
                String[] items = s.split("\t");  	                  
                // 暂时只需要 brandname 字段  
                String brandName = items[0];	                
                if(!brandList.contains(brandName)){  
                	brandList.add(brandName);  
                }  
            }  	  
            // 释放  
            if (input != null) {  
                input.close();  
                input = null;  
            }  
            if (fin != null) {  
                fin.close();  
                fin = null;  
            }  	        
 
		}                   
	        
		    @Override  
		    protected void setup(Context context) throws IOException {  
		        // 读取数据  
//		        brandFilePath = context.getConfiguration().get("hdfs://itcast:9000/ddd0");  
		    	brandFilePath = "hdfs://itcast01:9000/xiaoshou/part-r-00000";
		        readBrand(context);
		    }  
		    @Override  
		    protected void cleanup(Context context) throws IOException {  
		        // 释放数据  
		        if(!brandList.isEmpty()){  
		            brandList.clear();  
		        }
		    }		            
	        
			@Override
			protected void map(LongWritable key, Text value,Context context)
					throws IOException, InterruptedException {
//				ArrayList<String> brand =new ArrayList<String>();
				InputSplit inputSplit=(InputSplit)context.getInputSplit();
				String filename=((FileSplit)inputSplit).getPath().getName();
				String line = value.toString();
				String[] fields = line.split("@#@#");
	            int max = brandList.size();
				for (int i = 0;i<max;i++)
				{
					if(filename.indexOf(brandList.get(i))!=-1){
						context.write(new Text(brandList.get(i)), new Text(fields[fields.length-1]));
					    break;					
					}
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
			for(Text t : values){
				mos.write("MOSText","",t,key.toString()+"/");

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
		 
		job.setJarByClass(DataClassify.class);
		
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
