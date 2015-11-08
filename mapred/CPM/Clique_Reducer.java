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

public class Clique_Reducer extends Reducer<Text, Text, Text, Text> {

	private List<Integer> route = new LinkedList<Integer>();
	private Map<Integer, Set<Integer>> dict = new HashMap<Integer, Set<Integer>>();
	
	// 选出和其他邻居连接最多的结点
	public Integer maxNeighbor(Set<Integer> neighbors) {
		//System.out.println("neighbors: " + neighbors);
		if (neighbors.size() == 0)
			return null;
		int max = -1;
		int maxConnectTime = -1;
		for (int neighbor : neighbors) {
			int time = 0;
			// System.out.println("neighbor: " + neighbor);
			for (int secondNeighbor : dict.get(neighbor))
				if (neighbors.contains(secondNeighbor))
					time += 1;
			if (time > maxConnectTime) {
				maxConnectTime = time;
				max = neighbor;
			}
		}
		return max;
	}

	// 集合交
	public Set<Integer> join(Set<Integer> setA, Set<Integer> setB) {
		Set<Integer> newSet = new HashSet<Integer>();
		for (int node : setA)
			if (setB.contains(node))
				newSet.add(node);
		return newSet;
	}

	// 集合差
	public Set<Integer> minus(Set<Integer> setA, Set<Integer> setB) {
		Set<Integer> newSet = new HashSet<Integer>();
		for (int node : setA)
			if (!setB.contains(node))
				newSet.add(node);
		return newSet;
	}

	// 深度复制集合
	public Set<Integer> deepCopySet(Set<Integer> s) {
		Set<Integer> newSet = new HashSet<Integer>();
		for (int n : s)
			newSet.add(n);
		return newSet;
	}
	
	public List<Integer> deepCopyList(List<Integer> s) {
		List<Integer> newList = new LinkedList<Integer>();
		for (int n : s)
			newList.add(n);
		return newList;
	}

	// 打印团
	public String printClique() {
		boolean tag = true;
		for(int i=1;i<route.size();i++)
			if(route.get(0) > route.get(i))
				tag = false;   
		String res = "";
		if(tag) {
			res = route.toString();
			System.out.println("clique: " + route.toString());
		}
		return res;
	}

	public List<String> buildTree(int root, Set<Integer> neighbors,Set<Integer> hasVisited) {
		if (neighbors == null)
			return new LinkedList<String>();
		List<String> res = new LinkedList<String>();
		if (neighbors.size() == 0) {
			String clique = printClique();
			if(!"".equals(clique))
				res.add(clique);
		} else {
			int next = maxNeighbor(neighbors);
			Set<Integer> cand = minus(neighbors, dict.get(next));
			while (true) {
				cand.remove(next);
				route.add(next);
				if(!hasVisited.contains(next)) {
					hasVisited.add(next);
					res.addAll(buildTree(next, join(neighbors, dict.get(next)), deepCopySet(hasVisited)));
				}
				if (cand.size() == 0) {
					route.remove(route.size() - 1);
					break;
				}
				next = maxNeighbor(cand);
				route.remove(route.size() - 1);
			}
		}
		return res;
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		int root = Integer.valueOf(key.toString());
		route.add(root);
		Iterator<Text> iter = values.iterator();
		Set<Integer> directNeighbors = new HashSet<Integer>(); // the neighbors
																// of root
		while (iter.hasNext()) {
			String line = iter.next().toString();
			int index = line.indexOf(":");
			String neighbor = line.substring(0, index);
			directNeighbors.add(Integer.valueOf(neighbor));
			if (index == line.length() - 1) { // the neighbor only has one
												// neighbor which is the root
				/*Text clique = new Text();
				String s = "(" + root + "," + neighbor + ")";
				clique.set(s);
				System.out.println(s);
				// context.write(arg0, arg1);
*/				dict.put(Integer.valueOf(neighbor), new HashSet<Integer>());
				continue;
			}
			String[] secondNeighbors = line.substring(index + 1).split(",");
			// System.out.println(secondNeighbors);
			Set<Integer> s = new HashSet<Integer>();
			for (int i = 0; i < secondNeighbors.length; i++)
				s.add(Integer.valueOf(secondNeighbors[i]));
			dict.put(Integer.valueOf(neighbor), s);
		}
		//System.out.println(dict.toString());
		List<String> cliques = buildTree(root, directNeighbors, new HashSet<Integer>());
		for(String clique : cliques){
			Text t = new Text();
			t.set(clique);
			context.write(null,t);
		}
		route.remove(route.size()-1);
		//System.out.println("========================");
	}

	public static void main(String[] args) {
		Set<Integer> A = new HashSet<Integer>();
		Set<Integer> B = new HashSet<Integer>();
		A.add(1);
		A.add(3);
		A.add(5);
		B.add(1);
		// B.add(2);
		// B.add(3);
		// System.out.println(new CliqueReducer().join(A,B));
		System.out.println(new Clique_Reducer().maxNeighbor(B));
	}

}
