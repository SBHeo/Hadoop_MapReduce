package mju.hadoop.temp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
		private Text total = new Text("total");

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
					context.write(total, valueData);
				}
			}

		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable value = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
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

			if (!values[0].equals("total")) {
				key2.set(Integer.parseInt(values[1]));
				value2.set(values[0]);

				context.write(key2, value2);
			}
		}
	}

	public static class Compare2 extends WritableComparator {

		protected Compare2() {
			super(IntWritable.class, true);
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

	public static class Reduce2 extends Reducer<IntWritable, Text, Text, DoubleWritable> {
		private Text key2 = new Text();
		private DoubleWritable value2 = new DoubleWritable();

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			double total = Double.parseDouble(conf.get("total"));
			for (Text value : values) {
				key2.set(value);
			}
			int num = key.get();
			double result = num / total * 100;
			double result2 = Math.round(result*100d)/100d;
			value2.set(result2);
			context.write(key2, value2);
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

		String uri = "/user/hadoop/" + args[1] + "/part-r-00000";
		Configuration file = new Configuration();
		FileSystem fs = FileSystem.get(file);
		InputStream is = fs.open(new Path(uri));
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		String line;
		while ((line = reader.readLine()) != null) {
			String[] tmp = line.split("\t");
			if (tmp[0].equals("total")) {
				conf.set("total", tmp[1]);
			}
		}

		String input = args[1] + "/part-r-00000";
		String output = args[1] + "Sort";
		Job job2 = new Job(conf, "sort");
		job2.setJarByClass(project.class);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		job2.setSortComparatorClass(Compare2.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job2, new Path(input));
		FileOutputFormat.setOutputPath(job2, new Path(output));

		success = job2.waitForCompletion(true);
		System.out.println(success);
	}
}