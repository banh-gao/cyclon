package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.DataProcessor.GraphProperty;
import it.unitn.zozin.da.cyclon.DataProcessor.RoundData;
import it.unitn.zozin.da.cyclon.SimulationActor.Configuration;
import it.unitn.zozin.da.cyclon.SimulationActor.SimulationDataMessage;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class DataLogger {

	private final Configuration conf;
	private final Map<String, PrintWriter> openFiles = new HashMap<String, PrintWriter>();

	public DataLogger(Configuration conf) {
		this.conf = conf;
	}

	public void writeData(SimulationDataMessage data) {
		for (Entry<Integer, RoundData> e : data.simData.entrySet()) {
			int round = e.getKey();

			// Ignore boot round measure
			if (round == 0)
				continue;

			RoundData roundData = e.getValue();

			// Write all properties for current round
			for (GraphProperty prop : roundData.roundValues.keySet()) {
				writeRoundData(prop, roundData.roundValues.get(prop), round);
			}
		}

		for (PrintWriter f : openFiles.values())
			f.close();
	}

	void writeRoundData(GraphProperty prop, Object value, int round) {
		PrintWriter out = getFile(prop);
		out.write(prop.serializeData(value, round));
	}

	private PrintWriter getFile(GraphProperty prop) {
		String file = prop + "_" + conf.CYCLON_CACHE_SIZE + "_" + conf.BOOT_TOPOLOGY + ".dat";

		return openFiles.computeIfAbsent(file, (k) -> {
			try {
				return new PrintWriter(k);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		});
	}
}
