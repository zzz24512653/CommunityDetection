package clique;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class KMerge_Mapper extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		//System.out.println(line);
		List<Integer> clique = new LinkedList<Integer>();
		for(String node : line.substring(1,line.length()-1).split(",")){
			clique.add(Integer.valueOf(node.trim()));
		}
		
		int min = clique.get(0);
		for(int i = 1;i<clique.size();i++)
			if(clique.get(i) < min)
				min = clique.get(i);
	
		Text newKey = new Text();
		newKey.set(Integer.toString(min));
		context.write(newKey,value);
	}
}
