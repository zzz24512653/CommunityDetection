package pscan;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SCAN {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public String convert(String[] neighbors) {
			StringBuilder sb = new StringBuilder();
			for (String s : neighbors) {
				sb.append(s);
				sb.append(",");
			}
			return sb.substring(0, sb.length()-1).toString();
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			// System.out.println(line);
			int index = line.indexOf(":");
			if(index == -1)
				return;
			String node = line.substring(0, index);
			String[] neighbors = line.substring(index + 1).split(
					",");
			for (String neighbor : neighbors) {
				Text edge = new Text();
				if (Integer.valueOf(node) < Integer.valueOf(neighbor))
					edge.set(node + "," + neighbor);
				else
					edge.set(neighbor + "," + node);
				Text t = new Text();
				t.set(convert(neighbors));
				context.write(edge, t);
			}

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		/**
		 * 计算结点的结构化相似度
		 * @param s1
		 * @param s2
		 * @return
		 */
		public double calSim(Set<Integer> s1,Set<Integer> s2) {
			double common = 2.0;
			for(Integer i : s1)
				if(s2.contains(i))
					common += 1.0;
			return common/Math.sqrt((s1.size()+1)*(s2.size()+1));
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double alpha = Double.valueOf(context.getConfiguration().get("alpha"));
			//System.out.println("reduce alpha:" + alpha);
			Iterator<Text> iter = values.iterator();
			Set<Integer> set1 = new HashSet<Integer>();
			Set<Integer> set2 = new HashSet<Integer>();
			for(String s : iter.next().toString().split(","))
				set1.add(Integer.valueOf(s));
			try{
				for(String s : iter.next().toString().split(","))
					set2.add(Integer.valueOf(s));
			}catch(Exception e){
				System.out.println("Error: " + key);
			}
			double sim = calSim(set1, set2);
			//System.out.println(key+": "+sim);
			if(sim >= alpha){
				context.write(null,key);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		for(int iterTime=1;iterTime<10;iterTime++){
			//System.out.println("iterTime: "+iterTime);
			Job job = new Job();
			job.setJarByClass(SCAN.class);
			job.setJobName("scan");

			double alpha = iterTime/10.0;

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat
					.setOutputPath(job, new Path("hdfs://localhost/output/pscan_"+alpha));

			Configuration conf = job.getConfiguration();
			//System.out.println("main alpha:" + alpha);
			conf.set("alpha", Double.toString(alpha));
			
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			// job.setNumReduceTasks(4);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.waitForCompletion(true);
		}
		System.exit(0);
	}
}
