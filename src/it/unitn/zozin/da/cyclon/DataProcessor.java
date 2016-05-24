package it.unitn.zozin.da.cyclon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public class DataProcessor {

	private static final int DIST_UNREACHABLE = Integer.MAX_VALUE;

	public RoundData processRoundSample(Set<GraphProperty> params, boolean[][] graph) {
		Map<Integer, Integer> inDegreeDistr = new TreeMap<Integer, Integer>();
		float aggClustering = 0;
		int aggTotalDistance = 0;

		RoundData data = new RoundData();
		for (int node = 0; node < graph.length; node++) {
			if (params.contains(GraphProperty.PATH_LEN))
				aggTotalDistance += calcNodePathSum(node, graph);
			if (params.contains(GraphProperty.CLUSTERING))
				aggClustering += calcLocalClustering(node, graph);
			if (params.contains(GraphProperty.IN_DEGREE))
				inDegreeDistr.compute(calcInDegree(node, graph), (k, v) -> (v == null) ? 1 : v + 1);
		}

		if (params.contains(GraphProperty.PATH_LEN)) {
			float apl = aggTotalDistance / (float) (graph.length * (graph.length - 1));
			data.addData(GraphProperty.PATH_LEN, apl);
		}

		if (params.contains(GraphProperty.CLUSTERING)) {
			float clusteringCoeff = aggClustering / graph.length;
			data.addData(GraphProperty.CLUSTERING, clusteringCoeff);
		}
		if (params.contains(GraphProperty.IN_DEGREE))
			data.addData(GraphProperty.IN_DEGREE, inDegreeDistr);

		return data;

	}

	private int calcInDegree(int node, boolean[][] graph) {
		int nodeInDegree = 0;
		// Count nodes pointing to this node
		for (int neighbor = 0; neighbor < graph.length; neighbor++)
			if (graph[neighbor][node])
				nodeInDegree++;

		return nodeInDegree;
	}

	private float calcLocalClustering(int node, boolean[][] graph) {
		List<Integer> neighbors = new ArrayList<Integer>();

		// Get neighbors of the current node
		for (int neighbor = 0; neighbor < graph.length; neighbor++)
			if (graph[node][neighbor])
				neighbors.add(neighbor);

		// Graph induced by a node with less than two neighbors has 0 edges
		// clustering coefficient equals to 0
		if (neighbors.size() < 2)
			return 0;

		int edges = 0;

		for (int n1 : neighbors) {
			// Count the number of edges of the graph induced by the
			// current node (edges between current node neighbors)
			for (int n2 : neighbors) {
				// Skip the edge pointing to inducer node
				if (n2 == node)
					continue;

				if (graph[n1][n2])
					edges++;
			}
		}

		float local = edges / (float) (neighbors.size() * (neighbors.size() - 1));
		return local;
	}

	private int calcNodePathSum(int node, boolean[][] graph) {
		int nodeDist = 0;
		int[] dist = shortestPath(node, graph);
		for (int n2 = 0; n2 < dist.length; n2++) {
			// Ignore unreachable nodes
			if (dist[n2] == DIST_UNREACHABLE)
				continue;

			nodeDist += dist[n2];
		}
		return nodeDist;
	}

	private static int[] shortestPath(int src, boolean[][] graph) {
		int[] dist = new int[graph.length];
		boolean[] visited = new boolean[graph.length];

		for (int i = 0; i < graph.length; i++) {
			dist[i] = DIST_UNREACHABLE;
			visited[i] = false;
		}

		dist[src] = 0;

		for (int i = 0; i < graph.length; i++) {
			int minVertex = 0;
			int min = DIST_UNREACHABLE;
			for (int j = 0; j < graph.length; j++) {
				if (!visited[j] && dist[j] < min) {
					minVertex = j;
					min = dist[j];
				}
			}

			visited[minVertex] = true;

			for (int v = 0; v < graph.length; v++) {
				int edgeWeight = undirectedEdgeWeight(minVertex, v, graph);
				if (!visited[v] && edgeWeight != DIST_UNREACHABLE && dist[minVertex] != DIST_UNREACHABLE && dist[minVertex] + edgeWeight < dist[v])
					dist[v] = dist[minVertex] + edgeWeight;
			}
		}
		return dist;
	}

	private static int undirectedEdgeWeight(int nodeA, int nodeB, boolean[][] graph) {
		if (graph[nodeA][nodeB] || graph[nodeB][nodeA])
			return 1;
		else
			return DIST_UNREACHABLE;
	}

	public static class RoundData {

		public static final RoundData EMPTY_DATA = new RoundData();
		final Map<GraphProperty, Object> roundValues = new HashMap<DataProcessor.GraphProperty, Object>();

		public void addData(GraphProperty prop, Object data) {
			roundValues.put(prop, data);
		}

		@Override
		public String toString() {
			return "RoundData " + roundValues;
		}
	}

	public static enum GraphProperty {
		IN_DEGREE {

			@SuppressWarnings("unchecked")
			@Override
			String serializeData(Object inDegreeDistr, int round) {
				StringBuilder b = new StringBuilder();
				for (Entry<Integer, Integer> dist : ((Map<Integer, Integer>) inDegreeDistr).entrySet())
					b.append(dist.getKey() + " " + dist.getValue() + "\n");
				return b.toString();
			}
		},
		PATH_LEN {

			@Override
			String serializeData(Object value, int round) {
				return round + " " + value.toString() + "\n";
			}
		},
		CLUSTERING {

			@Override
			String serializeData(Object value, int round) {
				return round + " " + value.toString() + "\n";
			}
		};

		abstract String serializeData(Object value, int round);
	};

}
