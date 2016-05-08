package it.unitn.zozin.da.cyclon;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MetricAlgorithms {

	private static final int DIST_UNREACHABLE = Integer.MAX_VALUE;

	public static Map<Integer, Integer> calcInDegree(boolean[][] adjacencyMatrix) {
		Map<Integer, Integer> inDegreeDistr = new TreeMap<Integer, Integer>();

		for (int node = 0; node < adjacencyMatrix.length; node++) {
			int inDegree = 0;
			for (int neighbor = 0; neighbor < adjacencyMatrix.length; neighbor++) {
				if (adjacencyMatrix[neighbor][node])
					inDegree++;
			}
			int count = inDegreeDistr.getOrDefault(inDegree, 0);
			inDegreeDistr.put(inDegree, count + 1);
		}

		return inDegreeDistr;
	}

	public static float calcClusteringCoeff(boolean[][] adjacencyMatrix) {
		float global = 0;

		for (int node = 0; node < adjacencyMatrix.length; node++) {
			List<Integer> neighbors = new ArrayList<Integer>();
			for (int neighbor = 0; neighbor < adjacencyMatrix.length; neighbor++) {
				if (adjacencyMatrix[node][neighbor]) {
					neighbors.add(neighbor);
				}
			}

			// Node with less than two neighbors does not contribute to
			// clustering
			if (neighbors.size() < 2)
				continue;

			int edges = 0;

			for (int neighbor : neighbors) {
				for (int n2 = 0; n2 < adjacencyMatrix.length; n2++) {
					if (adjacencyMatrix[neighbor][n2]) {
						if (neighbors.contains(n2))
							edges++;
					}
				}
			}

			global += edges / (float) (neighbors.size() * (neighbors.size() - 1));
		}

		return global / adjacencyMatrix.length;
	}

	public static float calcAveragePathLength(boolean[][] adjacencyMatrix) {
		int totalDistance = 0;
		for (int node = 0; node < adjacencyMatrix.length; node++) {

			int[] dist = shortestPath(node, adjacencyMatrix);
			for (int d : dist) {
				if (d < DIST_UNREACHABLE) {
					totalDistance += d;
				}
			}

		}
		return totalDistance / (float) (adjacencyMatrix.length * (adjacencyMatrix.length - 1));
	}

	private static int[] shortestPath(int src, boolean[][] adjacencyMatrix) {
		// The output array dist[i] will hold
		// the shortest distance from src to i
		int[] dist = new int[adjacencyMatrix.length];

		// visited[i] will be true if vertex i is included in shortest
		// path tree or shortest distance from src to i is finalized
		boolean[] visited = new boolean[adjacencyMatrix.length];

		// Initialize all distances as INFINITE and visited[] as false
		for (int i = 0; i < adjacencyMatrix.length; i++) {
			dist[i] = DIST_UNREACHABLE;
			visited[i] = false;
		}

		// Distance of source vertex from itself is always 0
		dist[src] = 0;

		// Find shortest path for all vertices
		for (int i = 0; i < adjacencyMatrix.length; i++) {

			// Pick the minimum distance vertex from the set of vertices
			// not yet processed. minVertex is always equal to src in first
			// iteration.
			int minVertex = 0;
			int min = DIST_UNREACHABLE;
			for (int j = 0; j < adjacencyMatrix.length; j++) {
				// Update dist[v] only if is not in sptSet, there is an
				// edge from u to v, and total weight of path from src to
				// v through u is smaller than current value of dist[v]
				if (!visited[j] && dist[j] < min) {
					minVertex = j;
					min = dist[j];
				}
			}

			// Mark the picked vertex as processed
			visited[minVertex] = true;

			// Update dist value of the adjacent vertices of the
			// picked vertex.
			for (int v = 0; v < adjacencyMatrix.length; v++)
				// Update dist[v] only if is not in sptSet, there is an
				// edge from u to v, and total weight of path from src to
				// v through u is smaller than current value of dist[v]
				if (!visited[v] && adjacencyMatrix[minVertex][v] && dist[minVertex] != DIST_UNREACHABLE && dist[minVertex] + (adjacencyMatrix[minVertex][v] ? 1 : 0) < dist[v])
					dist[v] = dist[minVertex] + (adjacencyMatrix[minVertex][v] ? 1 : 0);
		}
		return dist;
	}
}
