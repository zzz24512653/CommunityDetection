package clique;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CK_Graph_Mapper extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		//System.out.println("ck line: " + line);
		String clique = line.split("\t")[1];
		for(String node : clique.substring(1,clique.length()-1).split(",")){
			Text newKey = new Text();
			newKey.set(node);
			//Text newValue = new Text();
			
			context.write(newKey,value);
		}
	}
}
