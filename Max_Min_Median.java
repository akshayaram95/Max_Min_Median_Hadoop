
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Max_Min_Median {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Pair_Writable_Minmax>{
	long one=1;
		private Pair_Writable_Minmax value1 = new Pair_Writable_Minmax();
		
		private Text number = new Text(); // type of output key
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
			number.set(value.toString());
			value1.set(Long.parseLong(value.toString()),one);
			context.write(number, value1);
			
		}
	}
	
	public static class CombineClass extends Reducer<Text, Pair_Writable_Minmax, Text, Pair_Writable_Minmax>{
		
		private Pair_Writable_Minmax value = new Pair_Writable_Minmax();
		private Text word=new Text();
		public void reduce(Text key, Iterable<Pair_Writable_Minmax> values, Context context) throws IOException, InterruptedException{
		
			long count = 0; 
			for(Pair_Writable_Minmax numberPair:values){
				count+=numberPair.getCount();
			}
			value.set(Long.parseLong(key.toString()),count);
			word.set("Number");
			context.write(word, value);
		}
	}
		
	public static class Reduce extends Reducer<Text,Pair_Writable_Minmax,Text,Text> {
		private Text word = new Text();
		private Text solution = new Text();
		private long max=0;
		private long min=0;
		private long median=0;
		public void reduce(Text key, Iterable<Pair_Writable_Minmax> values, Context context) throws IOException, InterruptedException{
			long count = 0;
			List<Pair_Writable_Minmax> elements = new ArrayList<Pair_Writable_Minmax>();
			for (Pair_Writable_Minmax pair:values){
				count += pair.getCount();
				elements.add(new Pair_Writable_Minmax(pair));
			}
			Collections.sort(elements);
			min=elements.get(0).getStar();
			max=elements.get(elements.size()-1).getStar();
			int check=(int)(count)/2;
			int set=0;
			for(int i=0; i<elements.size();i++) {
				if(set<check) {
					set+=elements.get(i).getCount();
				}
				else {
					median=elements.get(i).getStar();
					break;
				}
			}
			word.set(min+"\t"+max+"\t"+median);
			context.write(word, solution);
		}
	}
	
	public static void main(String[] args) throws Exception {
	  	Configuration conf = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  	if (otherArgs.length != 2) {
	  	System.err.println("Usage: Median <in> <out>");
	  	System.exit(2);
	  	}
	  	Job job = new Job(conf, "Max_Min_Median");
	  	job.setJarByClass(Max_Min_Median.class);
	  	job.setMapperClass(Map.class);
	  	job.setCombinerClass(CombineClass.class);
	  	job.setReducerClass(Reduce.class);
	  	job.setOutputKeyClass(Text.class);
	  	job.setOutputValueClass(Text.class);
	  	job.setMapOutputKeyClass(Text.class);
	  	job.setMapOutputValueClass(Pair_Writable_Minmax.class);
	  	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	  	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	  	System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
}