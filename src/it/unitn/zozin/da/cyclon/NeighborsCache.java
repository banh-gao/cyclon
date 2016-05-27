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

	private final List<Neighbor> cache;

	private final Random rand;
	Set<Integer> selectedEntries;

	public NeighborsCache(int maxSize) {
		selectedEntries = new HashSet<Integer>();
		this.MAX_SIZE = maxSize;
		this.rand = new Random();
		cache = new ArrayList<Neighbor>();
	}

	public void increaseNeighborsAge() {
		for (Neighbor n : cache)
			n.age += 1;
	}

	public Neighbor selectOldestNeighbor() {
		int oldestIndex = getOldestEntry();

		selectedEntries.add(oldestIndex);
		return cache.get(oldestIndex);
	}

	private int getOldestEntry() {
		int oldestAge = -1;
		int oldestIndex = -1;

		for (int i = 0; i < cache.size(); i++) {
			int age = cache.get(i).age;
			if (age > oldestAge) {
				oldestAge = age;
				oldestIndex = i;
			}
		}

		return oldestIndex;
	}

	public Neighbor getRandomNeighbor() {
		return cache.get(rand.nextInt(cache.size()));
	}

	public List<Neighbor> selectRandomNeighbors(int shuffleLength) {
		return selectRandomNeighbors(shuffleLength, null);
	}

	public List<Neighbor> selectRandomNeighbors(int shuffleLength, ActorRef exclude) {
		List<Neighbor> out = new ArrayList<Neighbor>();

		List<Integer> selected = IntStream.range(0, cache.size()).boxed().collect(Collectors.toList());
		Collections.shuffle(selected, rand);
		selected = selected.subList(0, Math.min(shuffleLength, selected.size()));

		selectedEntries.addAll(selected);

		for (Integer i : selected) {
			Neighbor n = cache.get(i);
			if (exclude == null || !n.address.equals(exclude))
				out.add(n);
		}

		return out;
	}

	public void updateNeighbors(Collection<Neighbor> newEntries, boolean overwriteOldest) {

		Iterator<Integer> replaceableEntriesIter = selectedEntries.iterator();

		for (Neighbor newNeighbor : newEntries) {
			// If the are no free cache slots, remove a replaceable entry before
			// inserting the new one

			int newEntryIndex = (cache.size() == 0) ? 0 : cache.size() - 1;

			if (freeSlots() == 0) {
				if (!replaceableEntriesIter.hasNext() && overwriteOldest) {
					newEntryIndex = getOldestEntry();
				} else {
					newEntryIndex = replaceableEntriesIter.next();
					replaceableEntriesIter.remove();
				}

				cache.remove(newEntryIndex);
			}

			cache.add(newEntryIndex, newNeighbor);
		}
	}

	@Override
	public String toString() {
		return "NeighborsCache " + cache;
	}

	/**
	 * Neighbor objects are sorted only by age (Comparable interface) but the
	 * equals method compares only the address (This class is inconsistent with
	 * the Comparable interface specification)
	 *
	 */
	public static class Neighbor implements Comparable<Neighbor>, Cloneable {

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

		@Override
		public Neighbor clone() {
			return new Neighbor(age, address);
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

	public List<Neighbor> getNeighbors() {
		return Collections.unmodifiableList(cache);
	}
}
