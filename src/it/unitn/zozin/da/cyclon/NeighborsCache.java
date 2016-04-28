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
	private final int MAX_SIZE;

	private final List<Neighbor> neighbors;

	private final Random rand;

	public NeighborsCache(int maxSize) {
		rand = new Random(SEED);
		this.MAX_SIZE = maxSize;

		neighbors = new ArrayList<Neighbor>();
	}

	public void increaseNeighborsAge() {
		for (Neighbor n : neighbors)
			n.age += 1;
	}

	public Neighbor getOldestNeighbor() {
		Collections.sort(neighbors);
		return neighbors.get(neighbors.size() - 1);
	}

	public List<Neighbor> getRandomNeighbors(int length) {
		List<Neighbor> out = new ArrayList<Neighbor>(neighbors);
		out.remove(getOldestNeighbor());
		Collections.shuffle(out, rand);
		if (out.size() > 1)
			out = out.subList(0, Math.min(length, out.size()) - 1);
		out.add(getOldestNeighbor());
		return out;
	}

	public void addNeighbors(Collection<Neighbor> newNeighbors, List<Neighbor> neighborsInRequest) {
		newNeighbors = new ArrayList<Neighbor>(newNeighbors);
		Iterator<Neighbor> i = newNeighbors.iterator();

		// Fill the cache first
		while (this.neighbors.size() < MAX_SIZE && i.hasNext()) {
			this.neighbors.add(i.next());
			i.remove();
		}

		// Save remaining new neighbor by replace all neighbors sent in request,
		// replacing old ones first
		while (!neighborsInRequest.isEmpty() && i.hasNext()) {
			Neighbor last = neighborsInRequest.remove(0);
			if (!this.neighbors.remove(last))
				this.neighbors.remove(getOldestNeighbor());

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
			return "(" + age + ", " + address.path().name() + ")";
		}

		@Override
		public int compareTo(Neighbor o2) {
			return age.compareTo(o2.age);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((address == null) ? 0 : address.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Neighbor other = (Neighbor) obj;
			if (address == null) {
				if (other.address != null)
					return false;
			} else if (!address.equals(other.address))
				return false;
			return true;
		}

	}

	@Override
	public String toString() {
		return "NeighborsCache " + neighbors;
	}

	public void remove(Neighbor node) {
		while (neighbors.remove(node));
	}
}
