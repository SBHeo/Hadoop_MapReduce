package mju.hadoop.project;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankMap extends Mapper<LongWritable, Text, IntWritable, Text> {
	private IntWritable keyData = new IntWritable();
	private Text valueData = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] data = line.split("\t");
		
		keyData.set(Integer.parseInt(data[1]));
		valueData.set(data[0]);
		
		context.write(keyData, valueData);
	}
}
