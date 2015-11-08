package clique;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Clique_Mapper extends Mapper<LongWritable, Text, Text, Text>{

	public String convert(String[] contents,int index) {
		StringBuffer sb = new StringBuffer();
		for(int i=0;i<contents.length;i++){
			if(i != index) {
				sb.append(contents[i]);
				sb.append(",");
			}
		}
		if(sb.length() != 0)
			sb.deleteCharAt(sb.length()-1);
		return sb.toString();
	}
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		System.out.println(line);
		int index = line.indexOf(":");
		if(index == -1)
			return;
		String node = line.substring(0, index);
		String[] neighbors = line.substring(index + 1).split(
				",");
		for (int i=0;i<neighbors.length;i++) {
			Text newKey = new Text();
			newKey.set(neighbors[i]);
			Text newLine = new Text();
			newLine.set(node+":"+convert(neighbors,i));
			//System.out.println(newKey.toString()+"-"+newLine.toString());
			context.write(newKey,newLine);
		}

	}
	
}
