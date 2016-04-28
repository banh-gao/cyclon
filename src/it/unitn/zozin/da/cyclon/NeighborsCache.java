package it.unitn.zozin.da.cyclon;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
		Neighbor oldest = neighbors.get(0);
		for (int i = 1; i < neighbors.size(); i++)
			if (neighbors.get(i).age > oldest.age)
				oldest = neighbors.get(i);

		return oldest;
	}

	public List<Neighbor> getRandomNeighbors(int shuffleLength) {
		return getRandomNeighbors(shuffleLength, null);
	}

	public List<Neighbor> getRandomNeighbors(int shuffleLength, Neighbor exclude) {
		List<Neighbor> out = new ArrayList<Neighbor>(neighbors);
		if (exclude != null)
			out.remove(exclude);

		Collections.shuffle(out, rand);

		return out.subList(0, Math.min(shuffleLength, out.size()));
	}

	public void updateNeighbors(Collection<Neighbor> newNeighbors, List<Neighbor> candidatesForReplacement, NodeActor node) {
		candidatesForReplacement = new ArrayList<Neighbor>(candidatesForReplacement);

		// INVARIANCE: all neighbor entries from incoming message will be stored
		// in cache <=> freeSlots + |replacement candidates| > 0
		assert newNeighbors.size() <= freeSlots() + candidatesForReplacement.size() : "NODE: " + node + " NEW: " + newNeighbors + " FREE: " + freeSlots() + " REPLACE: " + candidatesForReplacement;

		for (Neighbor newNeighbor : newNeighbors) {
			// If the are no free cache slots, remove a candidate neighbor
			// before inserting the new one
			if (freeSlots() == 0) {
				Neighbor cand = candidatesForReplacement.remove(0);

				// INVARIANCE: The replacement candidate has always to be
				// present in current neighbors (remove returns true if present)
				assert (this.neighbors.remove(cand)) : node + " " + cand + " not in cache: " + neighbors;
			}

			this.neighbors.add(newNeighbor);
		}

		// INVARIANCE: cache size never exceeds maximum size
		assert (neighbors.size() <= MAX_SIZE);
	}

	@Override
	public String toString() {
		return "NeighborsCache " + neighbors;
	}

	/**
	 * Neighbor objects are sorted only by age (Comparable interface) but the
	 * equals method compares only the address (This class is inconsistent with
	 * the Comparable interface specification)
	 *
	 */
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

	public int freeSlots() {
		return MAX_SIZE - neighbors.size();
	}
}
