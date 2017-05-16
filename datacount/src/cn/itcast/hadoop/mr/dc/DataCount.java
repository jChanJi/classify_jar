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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DataCount {

	public static class DCMapper extends Mapper<LongWritable, Text, Text, DataBean>{

		
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
//                String[] items = s.split("\t");  	                  
                // 暂时只需要 brandname 字段  
                String brandName = s;	                
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
		    	brandFilePath = "hdfs://itcast01:9000/brand/brand.txt";
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
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			String[] fields = line.split("@#@#");
			long tianmao = 0;
			long jingdong = 0;	

			InputSplit inputSplit=(InputSplit)context.getInputSplit();
			String filename=((FileSplit)inputSplit).getPath().getName();
			
			
			for (int i = 0;i<brandList.size();i++)
			{	
				String a = "京东"; 
				if(filename.indexOf(brandList.get(i))!=-1){
					if (fields[0].contains(a)) {
						
						jingdong = 1;
						DataBean bean = new DataBean(brandList.get(i), tianmao, jingdong);
						context.write(new Text(brandList.get(i)), bean);		
					}else{
						tianmao = 1;
						DataBean bean = new DataBean(brandList.get(i), tianmao, jingdong);
						context.write(new Text(brandList.get(i)), bean);
					}		
					break;					
				}				
			}						
		}		
	}
	
	public static class DCReducer extends Reducer<Text, DataBean, Text, DataBean>{

		@Override
		protected void reduce(Text key, Iterable<DataBean> values, Context context)
				throws IOException, InterruptedException {
			long tianmao_sum = 0;
			long jingdong_sum = 0;
			for(DataBean bean : values){
				tianmao_sum += bean.getTianmao();
				jingdong_sum += bean.getJingdong();
			}
			if(tianmao_sum!=0&&jingdong_sum!=0){
				DataBean bean = new DataBean("", tianmao_sum, jingdong_sum);
				context.write(key, bean);
			}			
		}		
	}	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(DataCount.class);
		
		job.setMapperClass(DCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataBean.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setReducerClass(DCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataBean.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
	

}
