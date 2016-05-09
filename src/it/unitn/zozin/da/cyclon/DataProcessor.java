package it.unitn.zozin.da.cyclon;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DataProcessor {

	private static final int DIST_UNREACHABLE = Integer.MAX_VALUE;

	public SimulationDataMessage processSample(boolean[][] adjMatrix) {
		return new SimulationDataMessage(adjMatrix.length, calcInDegree(adjMatrix), calcClusteringCoeff(adjMatrix), calcAveragePathLength(adjMatrix));
	}

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
			for (int neighbor = 0; neighbor < adjacencyMatrix.length; neighbor++)
				if (adjacencyMatrix[node][neighbor])
					neighbors.add(neighbor);

			// Node with less than two neighbors does not contribute to
			// clustering
			if (neighbors.size() < 2)
				continue;

			int edges = 0;

			for (int neighbor : neighbors)
				for (int n2 = 0; n2 < adjacencyMatrix.length; n2++)
					if (adjacencyMatrix[neighbor][n2] && neighbors.contains(n2))
						edges++;

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
		int[] dist = new int[adjacencyMatrix.length];
		boolean[] visited = new boolean[adjacencyMatrix.length];

		for (int i = 0; i < adjacencyMatrix.length; i++) {
			dist[i] = DIST_UNREACHABLE;
			visited[i] = false;
		}

		dist[src] = 0;

		for (int i = 0; i < adjacencyMatrix.length; i++) {
			int minVertex = 0;
			int min = DIST_UNREACHABLE;
			for (int j = 0; j < adjacencyMatrix.length; j++) {
				if (!visited[j] && dist[j] < min) {
					minVertex = j;
					min = dist[j];
				}
			}

			visited[minVertex] = true;

			for (int v = 0; v < adjacencyMatrix.length; v++)
				if (!visited[v] && adjacencyMatrix[minVertex][v] && dist[minVertex] != DIST_UNREACHABLE && dist[minVertex] + (adjacencyMatrix[minVertex][v] ? 1 : 0) < dist[v])
					dist[v] = dist[minVertex] + (adjacencyMatrix[minVertex][v] ? 1 : 0);
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
