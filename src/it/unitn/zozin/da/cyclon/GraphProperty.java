package it.unitn.zozin.da.cyclon;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public enum GraphProperty {
	IN_DEGREE {

		@SuppressWarnings("unchecked")
		@Override
		String serializeData(Object inDegreeDistr, int round) {
			StringBuilder b = new StringBuilder();
			for (Entry<Integer, Integer> dist : ((Map<Integer, Integer>) inDegreeDistr).entrySet())
				b.append(dist.getKey() + " " + dist.getValue() + "\n");
			return b.toString();
		}

		@Override
		Object calculate(int node, boolean[][] graph) {
			// Count nodes pointing to this node
			int nodeInDegree = 0;
			for (int neighbor = 0; neighbor < graph.length; neighbor++)
				if (graph[neighbor][node])
					nodeInDegree++;

			return nodeInDegree;
		}

		@Override

		@SuppressWarnings("unchecked")
		Object aggregate(Object inDegreeDistr, Object nodeInDegree) {
			if (inDegreeDistr == null)
				inDegreeDistr = new TreeMap<Integer, Integer>();
			((Map<Integer, Integer>) inDegreeDistr).compute((Integer) nodeInDegree, (k, v) -> (v == null) ? 1 : v + 1);
			return inDegreeDistr;
		}

		@Override
		Object computeFinal(Object inDegreeDistr, int totalNodes) {
			return inDegreeDistr;
		}
	},
	PATH_LEN {

		private static final int DIST_UNREACHABLE = Integer.MAX_VALUE;

		@Override
		String serializeData(Object value, int round) {
			return round + " " + value.toString() + "\n";
		}

		@Override
		Object calculate(int node, boolean[][] graph) {
			// Calculate the sum of all the shortest paths from this node
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

		// Dijkstra shortest path algorithm
		private int[] shortestPath(int src, boolean[][] graph) {
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
					int edgeWeight = graph[minVertex][v] ? 1 : DIST_UNREACHABLE;
					if (!visited[v] && edgeWeight != DIST_UNREACHABLE && dist[minVertex] != DIST_UNREACHABLE && dist[minVertex] + edgeWeight < dist[v])
						dist[v] = dist[minVertex] + edgeWeight;
				}
			}
			return dist;
		}

		@Override
		Object aggregate(Object aggTotalDistance, Object nodePathsSum) {
			if (aggTotalDistance == null)
				return nodePathsSum;
			return ((Integer) aggTotalDistance) + (Integer) nodePathsSum;
		}

		@Override
		Object computeFinal(Object aggTotalDistance, int totalNodes) {
			return (Integer) aggTotalDistance / (float) (totalNodes * (totalNodes - 1));
		}

	},
	CLUSTERING {

		@Override
		String serializeData(Object value, int round) {
			return round + " " + value.toString() + "\n";
		}

		@Override
		Object calculate(int node, boolean[][] graph) {
			List<Integer> neighbors = new ArrayList<Integer>();

			// Get neighbors of the current node
			for (int neighbor = 0; neighbor < graph.length; neighbor++)
				if (graph[node][neighbor])
					neighbors.add(neighbor);

			// Graph induced by a node with less than two neighbors has 0
			// edges
			// thus the clustering coefficient equals to 0
			if (neighbors.size() < 2)
				return 0f;

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

		@Override
		Object aggregate(Object aggTotalClustering, Object nodeLocalClustering) {
			if (aggTotalClustering == null)
				return nodeLocalClustering;
			return ((float) aggTotalClustering) + (float) nodeLocalClustering;
		}

		@Override
		Object computeFinal(Object aggTotalClustering, int totalnodes) {
			return (float) aggTotalClustering / totalnodes;
		}
	};

	abstract String serializeData(Object value, int round);

	abstract Object calculate(int node, boolean[][] graph);

	abstract Object aggregate(Object aggregated, Object newValue);

	abstract Object computeFinal(Object tempCalcValue, int totalNodes);
}
