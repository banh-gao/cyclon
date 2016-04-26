package it.unitn.zozin.da.cyclon;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import akka.actor.ActorRef;

public class NeighborsCache {

	public static final long SEED = 1234l;

	private final List<Neighbor> neighbors;
	private final int cacheSize;
	private final int shuffleLength;

	private final Random rand;

	public NeighborsCache(int cacheSize, int shuffleLength) {
		neighbors = new ArrayList<Neighbor>();
		rand = new Random(SEED);

		this.cacheSize = cacheSize;
		this.shuffleLength = shuffleLength;
	}

	public void increaseNeighborsAge() {
		for (Neighbor n : neighbors)
			n.age += 1;
	}

	public Neighbor getOldestNeighbor() {
		Collections.sort(neighbors);
		return neighbors.get(neighbors.size() - 1);
	}

	public List<Neighbor> getRandomNeighbors() {
		List<Neighbor> out = new ArrayList<Neighbor>(neighbors);

		out.remove(getOldestNeighbor());

		Collections.shuffle(out, rand);

		out = out.subList(0, Math.min(shuffleLength, neighbors.size()) - 1);

		out.add(getOldestNeighbor());

		return out;
	}

	public void addNeighbors(Collection<Neighbor> newNeighbors, List<Neighbor> neighborsInRequest) {
		newNeighbors = new ArrayList<Neighbor>(newNeighbors);
		Iterator<Neighbor> i = newNeighbors.iterator();

		// Fill the cache first
		while (this.neighbors.size() < cacheSize && i.hasNext()) {
			this.neighbors.add(i.next());
			i.remove();
		}

		// Save remaining new neighbor by replace all neighbors sent in request,
		// replacing old ones first
		while (!neighborsInRequest.isEmpty() && i.hasNext()) {
			Neighbor last = neighborsInRequest.remove(0);
			this.neighbors.remove(last);

			if (this.neighbors.add(i.next()))
				i.remove();
		}

		assert (newNeighbors.isEmpty());
	}

	public static class Neighbor implements Comparable<Neighbor> {

		Integer age;
		ActorRef address;

		public Neighbor(int age, ActorRef address) {
			this.age = age;
			this.address = address;
		}

		@Override
		public String toString() {
			return "Neighbor [age=" + age + ", address=" + address + "]";
		}

		@Override
		public int compareTo(Neighbor o2) {
			return age.compareTo(o2.age);
		}

	}

	@Override
	public String toString() {
		return "NeighborsCache [neighbors=" + neighbors + "]";
	}
}
