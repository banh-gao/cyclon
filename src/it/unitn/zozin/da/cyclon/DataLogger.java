package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.DataProcessor.GraphProperty;
import it.unitn.zozin.da.cyclon.SimulationActor.Configuration;
import it.unitn.zozin.da.cyclon.SimulationActor.SimulationDataMessage;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Map.Entry;

public class DataLogger {

	public static void writeData(SimulationDataMessage data) throws IOException {
		writeInDegree(data);
	}

	@SuppressWarnings("unchecked")
	static void writeInDegree(SimulationDataMessage data) throws IOException {
		String file = getFileName(GraphProperty.IN_DEGREE, data.conf);
		PrintWriter out = new PrintWriter(file);
		Map<Integer, Integer> inDegreeDistr = (Map<Integer, Integer>) data.simData.get(data.conf.ROUNDS).roundValues.get(GraphProperty.IN_DEGREE);
		for (Entry<Integer, Integer> dist : inDegreeDistr.entrySet())
			out.println(dist.getKey() + " " + dist.getValue());
		out.close();
	}

	private static String row(GraphProperty prop, Object value) {
		switch (prop) {
			case IN_DEGREE :
				return "in-degree,nodes";
			case CLUSTERING :
				return "round,clustering coefficient";
			case PATH_LEN :
				return "round,average path length";
			default :
				throw new IllegalStateException();
		}
	}

	private static String getFileName(GraphProperty prop, Configuration conf) {
		return prop + "_" + conf.CYCLON_CACHE_SIZE + "_" + conf.BOOT_TOPOLOGY + ".dat";
	}
}
