package clique;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Executor {

	public static void main(String[] args) throws Exception {
		// Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		//step-one  get all max clique
		Job cliqueGenerator = new Job();
		cliqueGenerator.setJarByClass(Executor.class);
		cliqueGenerator.setJobName("clique generator");

		FileInputFormat.addInputPath(cliqueGenerator, new Path(args[0]));
		FileOutputFormat.setOutputPath(cliqueGenerator,
				new Path("hdfs://localhost/output/allCliques"));

		cliqueGenerator.setMapperClass(Clique_Mapper.class);
		cliqueGenerator.setReducerClass(Clique_Reducer.class);
		// job.setNumReduceTasks(4);
		cliqueGenerator.setOutputKeyClass(Text.class);
		cliqueGenerator.setOutputValueClass(Text.class);

		cliqueGenerator.waitForCompletion(true);
		
		//step-two	initial merge
		Job iniMerger = new Job();
		iniMerger.setJarByClass(Executor.class);
		iniMerger.setJobName("initial merger");

		FileInputFormat.addInputPath(iniMerger, new Path("hdfs://localhost/output/allCliques"));
		FileOutputFormat.setOutputPath(iniMerger,
				new Path("hdfs://localhost/output/iniMerged"));

		iniMerger.setMapperClass(KMerge_Mapper.class);
		iniMerger.setReducerClass(KMerge_Reducer.class);
		// job.setNumReduceTasks(4);
		iniMerger.setOutputKeyClass(Text.class);
		iniMerger.setOutputValueClass(Text.class);

		iniMerger.waitForCompletion(true);
		
		//step-three construct ck-graph
		Job graphConstructor = new Job();
		graphConstructor.setJarByClass(Executor.class);
		graphConstructor.setJobName("ck-graph constructor");

		FileInputFormat.addInputPath(graphConstructor, new Path("hdfs://localhost/output/iniMerged"));
		FileOutputFormat.setOutputPath(graphConstructor,
				new Path("hdfs://localhost/output/ck-graph"));

		graphConstructor.setMapperClass(CK_Graph_Mapper.class);
		graphConstructor.setReducerClass(CK_Graph_Reducer.class);
		// job.setNumReduceTasks(4);
		graphConstructor.setOutputKeyClass(Text.class);
		graphConstructor.setOutputValueClass(Text.class);

		graphConstructor.waitForCompletion(true);
		
		
		System.exit(0);
	}
	
}
