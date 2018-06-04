package mju.hadoop.temp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class project {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text keyData = new Text();
		private IntWritable valueData = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] values = line.split(","); // 1 - time , 7 - color
			
			String[] time = values[1].split(":", 7); // 0 - h, 1 - m, 2 - s
			int hour = Integer.parseInt(time[0]);
			if (hour >= 20 || hour < 6) {
				if (!values[7].equals("0")) {
					keyData.set(values[7]);
					context.write(keyData, valueData);
				}
			}
			
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable value = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values)
				sum += value.get();
			value.set(sum);
			context.write(key, value);
		}
	}
	
	public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable key2 = new IntWritable();
		private Text value2 = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] values = line.split("\t");
			
			key2.set(Integer.parseInt(values[1]));
			value2.set(values[0]);
			
			context.write(key2, value2);
		}
	}
	
	public static class Compare2 extends WritableComparator {
		
		protected Compare2(){
			super(IntWritable.class,true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			IntWritable k1 = (IntWritable) o1;
			IntWritable k2 = (IntWritable) o2;
			
			int cmp = k1.compareTo(k2);
			
			return -1 * cmp;
		}	
	}

	public static class Reduce2 extends Reducer<IntWritable, Text, Text, IntWritable> {
		private Text key2 = new Text();

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text value : values){
				key2.set(value);
			}
			context.write(key2, key);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "count");
		job.setJarByClass(project.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean success = job.waitForCompletion(true);
		System.out.println(success);
		
		String intput = args[1]+"/part-r-00000";
		String output = args[1]+"Sort";
		Job job2 = new Job(conf, "sort");
		job2.setJarByClass(project.class);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		job2.setSortComparatorClass(Compare2.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job2, new Path(intput));
		FileOutputFormat.setOutputPath(job2, new Path(output));
		
		success = job2.waitForCompletion(true);
		System.out.println(success);
	}
}