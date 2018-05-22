package mju.hadoop.project;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RankReduce extends Reducer<IntWritable, Text, Text, IntWritable> {
	private Text value = new Text();

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for(Text text : values){
			value.set(text);
		}
		
		context.write(value, key);
	}
}
