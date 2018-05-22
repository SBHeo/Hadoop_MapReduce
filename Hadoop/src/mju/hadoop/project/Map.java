package mju.hadoop.project;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	private Text keyData = new Text();
	private IntWritable valueData = new IntWritable();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String check = conf.get("check");
		
		String line = value.toString();
		String[] values = line.split(",");
		int casualty = Integer.parseInt(values[5]) + Integer.parseInt(values[6]);
		
		if(check.equals("-year")){
			String[] date = values[1].split("-");
			keyData.set(date[0]);
		}else if(check.equals("-month")){
			String[] date = values[1].split("-");
			keyData.set(date[1]);
		}else if(check.equals("-state")){
			keyData.set(values[2]);
		}
		
		valueData.set(casualty);
		
		context.write(keyData, valueData);
	}
}
