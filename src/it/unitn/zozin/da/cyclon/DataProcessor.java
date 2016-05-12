package it.unitn.zozin.da.cyclon;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DataProcessor {

	private static final int DIST_UNREACHABLE = Integer.MAX_VALUE;

	public SimulationDataMessage processSample(boolean[][] graph) {
		Map<Integer, Integer> inDegreeDistr = new TreeMap<Integer, Integer>();

		float aggClustering = 0;

		int aggTotalDistance = 0;

		for (int node = 0; node < graph.length; node++) {
			// /////// In-degree distribution ////////////

			int nodeInDegree = 0;
			// Count nodes pointing to this node
			for (int neighbor = 0; neighbor < graph.length; neighbor++)
				if (graph[neighbor][node])
					nodeInDegree++;
			inDegreeDistr.compute(nodeInDegree, (k, v) -> (v == null) ? 1 : v + 1);

			// /////// Global Clustering coefficient ////////////

			aggClustering += calcLocalClustering(node, graph);

			// /////// Average path length (using Dijkstra) ////////////

			int[] dist = shortestPath(node, graph);
			for (int d : dist)
				if (d < DIST_UNREACHABLE)
					aggTotalDistance += d;
		}

		float clusteringCoeff = aggClustering / graph.length;

		float apl = aggTotalDistance / (float) (graph.length * (graph.length - 1));

		return new SimulationDataMessage(graph.length, inDegreeDistr, clusteringCoeff, apl);
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
				// Skip edges pointing to current node and to n1 itself
				if (n2 == node || n2 == n1)
					continue;

				if (graph[n1][n2])
					edges++;
			}
		}

		return edges / (float) neighbors.size() * (neighbors.size() - 1);
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

			for (int v = 0; v < graph.length; v++)
				if (!visited[v] && graph[minVertex][v] && dist[minVertex] != DIST_UNREACHABLE && dist[minVertex] + (graph[minVertex][v] ? 1 : 0) < dist[v])
					dist[v] = dist[minVertex] + (graph[minVertex][v] ? 1 : 0);
		}
		return dist;
	}

	public static class SimulationDataMessage {

		final Map<Integer, Integer> inDegreeDistr;
		final float clusteringCoeff;
		final float apl;
		final int totalNodes;

		public SimulationDataMessage(int totalNodes, Map<Integer, Integer> degreeDistr, float clusteringCoeff, float apl) {
			this.totalNodes = totalNodes;
			this.inDegreeDistr = degreeDistr;
			this.clusteringCoeff = clusteringCoeff;
			this.apl = apl;
		}

		@Override
		public String toString() {
			return "SimulationDataMessage [inDegreeDistr=" + inDegreeDistr + ", clusteringCoeff=" + clusteringCoeff + ", apl=" + apl + ", totalNodes=" + totalNodes + "]";
		}

	}
}
