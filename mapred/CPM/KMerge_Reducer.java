package clique;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMerge_Reducer extends Reducer<Text, Text, Text, Text> {
	
	private static int cliqueID = 1;
	
	//判断两个团是否临接
	public boolean isAdjacent(Set<Integer> c1,Set<Integer> c2) {
		int common = 0;
		for(int n1 : c1)
			if(c2.contains(n1))
				common++;
		int min = c1.size();
		if(c2.size() < min)
			min = c2.size();
		
		if(common >= min-1)
			return true;
		else 
			return false;
	}
	
	/**
	 * 已经合并的团是否包含与新团临接的团
	 * @param cliques
	 * @param clique
	 * @return -1表示不包含	其他值表示与指定团临接的团的index
	 */
	public int contain(List<Set<Integer>> cliques, Set<Integer> clique) {
		for(int i=0;i<cliques.size();i++) {
			if(isAdjacent(cliques.get(i), clique))
				return i;
		}
		return -1;
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		List<Set<Integer>> cliques = new LinkedList<Set<Integer>>();
		Iterator<Text> iter = values.iterator();
		while(iter.hasNext()){
			String line = iter.next().toString();
			Set<Integer> clique = new HashSet<Integer>();
			for(String node : line.substring(1,line.length()-1).split(",")){
				clique.add(Integer.valueOf(node.trim()));
			}
			cliques.add(clique);
		}
		
		List<Set<Integer>> mergedClique = new LinkedList<Set<Integer>>();
		for(Set<Integer> clique : cliques) {
			int index = contain(mergedClique,clique);
			if(index == -1)
				mergedClique.add(clique);
			else
				mergedClique.get(index).addAll(clique);
		}
		
		for(Set<Integer> clique : mergedClique){
			Text newKey = new Text();
			newKey.set(Integer.toString(cliqueID++));
			Text t = new Text();
			t.set(clique.toString());
			context.write(newKey, t);
		}
	}

	public static void main(String[] args) {
		
	}

}
