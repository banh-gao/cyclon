package it.unitn.zozin.da.cyclon;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public enum GraphProperty {
	IN_DEGREE {

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
		public Collector<Object, ?, Map<Object, Long>> collector() {
			return Collectors.groupingBy(Function.identity(), Collectors.counting());
		}

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

		private static final int DIST_UNREACHABLE = Integer.MAX_VALUE;

		@Override
		Object calculate(int node, boolean[][] graph) {
			// Calculate the sum of all the shortest paths from this node
			float nodeDist = 0;
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
		public Collector<Float, Float[], Float> collector() {
			return new AVGCollector((state) -> state[0] / (state[1] * (state[1] - 1)));
		}

		@Override
		String serializeData(Object value, int round) {
			return round + " " + value.toString() + "\n";
		}

	},
	CLUSTERING {

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
		public Collector<Float, Float[], Float> collector() {
			return new AVGCollector((state) -> state[0] / state[1]);
		}

		@Override
		String serializeData(Object value, int round) {
			return round + " " + value.toString() + "\n";
		}
	};

	/**
	 * Convert data to string for file output
	 * 
	 * @param value
	 * @param round
	 */
	abstract String serializeData(Object value, int round);

	/**
	 * Calculate value on single node
	 * 
	 * @param node
	 * @param graph
	 * @return
	 */
	abstract Object calculate(int node, boolean[][] graph);

	/**
	 * Compute final value by combing values from single nodes
	 * 
	 * @return
	 */
	public abstract Collector<?, ?, ?> collector();
}

class AVGCollector implements Collector<Float, Float[], Float> {

	private final Function<Float[], Float> finisher;

	public AVGCollector(Function<Float[], Float> finisher) {
		this.finisher = finisher;
	}

	@Override
	public Supplier<Float[]> supplier() {
		return () -> new Float[]{0f, 0f};
	}

	@Override
	public BiConsumer<Float[], Float> accumulator() {
		// state[0] = sum of values, state[1] = num of elements so far
		return (state, v) -> {
			state[0] += v;
			state[1]++;
		};
	}

	@Override
	public BinaryOperator<Float[]> combiner() {
		return null;
	}

	@Override
	public Function<Float[], Float> finisher() {
		return finisher;
	}

	@Override
	public Set<java.util.stream.Collector.Characteristics> characteristics() {
		return EnumSet.of(Characteristics.UNORDERED);
	}
};
