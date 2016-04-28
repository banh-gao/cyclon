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

		if (out.size() > 1)
			out = out.subList(0, Math.min(shuffleLength, out.size()));
		return out;
	}

	public void updateNeighbors(Collection<Neighbor> newNeighbors, List<Neighbor> candidatesForReplacement) {
		candidatesForReplacement = new ArrayList<Neighbor>(candidatesForReplacement);

		System.out.println("NEW: " + newNeighbors.size() + " FREE SLOTS: " + (MAX_SIZE - neighbors.size()) + " REPLACEABLE: " + candidatesForReplacement.size());

		if (MAX_SIZE - neighbors.size() + candidatesForReplacement.size() == 0)
			throw new IllegalStateException("NEW: " + newNeighbors.size() + " FREE SLOTS: " + (MAX_SIZE - neighbors.size()) + " REPLACEABLE: " + candidatesForReplacement.size());

		for (Neighbor newNeighbor : newNeighbors) {
			// If the cache is full, remove a candidate neighbor before
			// inserting the new one
			if (this.neighbors.size() == MAX_SIZE) {
				Neighbor last = candidatesForReplacement.remove(0);
				if (!this.neighbors.remove(last))
					throw new IllegalStateException(last + " NOT CONTAINED!");
			}

			this.neighbors.add(newNeighbor);
		}

		assert (newNeighbors.isEmpty());
		assert (neighbors.size() <= MAX_SIZE);
	}

	@Override
	public String toString() {
		return "NeighborsCache " + neighbors;
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

	public int size() {
		return neighbors.size();
	}
}
