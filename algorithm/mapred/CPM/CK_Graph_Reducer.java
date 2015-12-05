package clique;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CK_Graph_Reducer extends Reducer<Text, Text, Text, Text> {

	// 判断两个团是否临接
	public boolean isAdjacent(Set<Integer> c1, Set<Integer> c2) {
		int common = 0;
		for (int n1 : c1)
			if (c2.contains(n1))
				common++;
		int min = c1.size();
		if (c2.size() < min)
			min = c2.size();

		if (common >= min - 1)
			return true;
		else
			return false;
	}

	/**
	 * 已经合并的团是否包含与新团临接的团
	 * 
	 * @param cliques
	 * @param clique
	 * @return -1表示不包含 其他值表示与指定团临接的团的index
	 */
	public int contain(List<Set<Integer>> cliques, Set<Integer> clique) {
		for (int i = 0; i < cliques.size(); i++) {
			if (isAdjacent(cliques.get(i), clique))
				return i;
		}
		return -1;
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		List<Set<Integer>> cliques = new LinkedList<Set<Integer>>();
		Map<Integer, Integer> index_cliqueID = new HashMap<Integer, Integer>();
		Iterator<Text> iter = values.iterator();
		while (iter.hasNext()) {
			String[] contents = iter.next().toString().split("\t");
			int cliqueID = Integer.valueOf(contents[0]);
			Set<Integer> clique = new HashSet<Integer>();
			for (String node : contents[1].substring(1,
					contents[1].length() - 1).split(",")) {
				clique.add(Integer.valueOf(node.trim()));
			}
			cliques.add(clique);
			index_cliqueID.put(cliques.size() - 1, cliqueID);
		}

		if (cliques.size() == 1)
			return;

		for (int i = 0; i < cliques.size(); i++)
			for (int j = i + 1; j < cliques.size(); j++)
				if (isAdjacent(cliques.get(i), cliques.get(j))) {
					int clique_i = index_cliqueID.get(i);
					int clique_j = index_cliqueID.get(j);
					Text t = new Text();
					if(clique_i < clique_j)
						t.set(clique_i+","+clique_j);
					else
						t.set(clique_j+","+clique_i);
					context.write(null, t);
				}
	}

	public static void main(String[] args) {

	}

}
