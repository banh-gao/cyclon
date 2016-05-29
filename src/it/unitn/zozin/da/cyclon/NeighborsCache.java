package it.unitn.zozin.da.cyclon;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import akka.actor.ActorRef;

public class NeighborsCache {

	private final int MAX_SIZE;
	private final Random rand;

	private final Set<Neighbor> cache;

	public NeighborsCache(int maxSize) {
		this.MAX_SIZE = maxSize;
		this.rand = new Random();
		cache = new HashSet<Neighbor>(maxSize);
	}

	public void increaseNeighborsAge() {
		for (Neighbor n : cache)
			n.age += 1;
	}

	public Neighbor getRandomNeighbor() {
		// FIXME: use faster implementation
		Iterator<Neighbor> i = cache.iterator();
		Neighbor n = i.next();

		int v = rand.nextInt(cache.size());

		for (int p = 1; p < v; p++)
			n = i.next();

		return n;
	}

	public int addNeighbors(Collection<Neighbor> newEntries) {
		int added = 0;
		for (Neighbor n : newEntries) {
			// Stop when there are no free slots anymore
			if (freeSlots() == 0)
				break;

			// Add only if not already contained in cache
			if (cache.add(n))
				added++;
		}
		return added;
	}

	/**
	 * Remove up to max random neigbhors from the cache
	 * 
	 * @param max
	 * @return The removed neighbors
	 */
	public List<Neighbor> removeRandomNeighbors(int max) {
		return removeRandomNeighbors(max, null);
	}

	/**
	 * Remove up to max random neigbhors from the cache. The specified element
	 * is excluded from removal.
	 * 
	 * @param max
	 * @param exclude
	 * @return The removed neighbors
	 */
	public List<Neighbor> removeRandomNeighbors(int max, ActorRef exclude) {
		List<Integer> toRemove = IntStream.range(0, cache.size()).boxed().collect(Collectors.toList());
		Collections.shuffle(toRemove, rand);
		// FIXME: remove excluded already here
		toRemove = toRemove.subList(0, Math.min(max, toRemove.size()));

		List<Neighbor> removed = new ArrayList<Neighbor>();
		int i = 0;
		for (Neighbor n : cache)
			if (toRemove.contains(i++) && (exclude == null || !n.address.equals(exclude)))
				removed.add(n);

		cache.removeAll(removed);

		return removed;

	}

	public Neighbor removeOldestNeighbor() {
		Neighbor oldest = null;

		for (Neighbor n : cache) {
			if (oldest == null || n.age > oldest.age) {
				oldest = n;
			}
		}

		cache.remove(oldest);

		return oldest;
	}

	@Override
	public String toString() {
		return "NeighborsCache " + cache;
	}

	/**
	 * Neighbor objects equals method compares only the address (This class is
	 * inconsistent with the Comparable interface specification)
	 *
	 */
	public static class Neighbor implements Cloneable {

		Integer age;
		final ActorRef address;

		public Neighbor(int age, ActorRef address) {
			this.age = age;
			this.address = address;
		}

		@Override
		public String toString() {
			return "(" + age + ", " + address.path().name() + ")";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((address == null) ? 0 : address.hashCode());
			return result;
		}

		@Override
		public Neighbor clone() {
			return new Neighbor(age, address);
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
		return MAX_SIZE - cache.size();
	}

	public int size() {
		return cache.size();
	}

	public int maxSize() {
		return MAX_SIZE;
	}

	public Set<Neighbor> getNeighbors() {
		return Collections.unmodifiableSet(cache);
	}
}
