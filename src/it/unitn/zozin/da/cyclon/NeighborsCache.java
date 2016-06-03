package it.unitn.zozin.da.cyclon;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import akka.actor.ActorRef;

/**
 * Neighbor cache to support nodes for Cyclon protocol
 */
class NeighborsCache {

	private final int MAX_SIZE;
	private final Random rand;

	private final Map<ActorRef, Integer> cache;

	public NeighborsCache(int maxSize) {
		this.MAX_SIZE = maxSize;
		this.rand = new Random();
		cache = new HashMap<>(MAX_SIZE);
	}

	/**
	 * Increase age of all entries in cache
	 */
	public void increaseNeighborsAge() {
		cache.replaceAll((k, v) -> v + 1);
	}

	/**
	 * Add the specied values to free slots
	 * Excludes duplicated addresses
	 * 
	 * @param newEntries
	 */
	public void addNeighbors(Map<ActorRef, Integer> newEntries) {
		for (Entry<ActorRef, Integer> n : newEntries.entrySet()) {
			// Stop when there are no more free slots
			if (MAX_SIZE - cache.size() == 0)
				break;

			cache.putIfAbsent(n.getKey(), n.getValue());
		}
	}

	public ActorRef getRandomNeighbor() {
		return cache.keySet().stream().limit(rand.nextInt(cache.size()) + 1).reduce((f, s) -> s).get();
	}

	/**
	 * Remove up to max random neigbhors from the cache
	 * 
	 * @param max
	 * @return The removed neighbors
	 */
	public Map<ActorRef, Integer> removeRandomNeighbors(int max) {
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
	public Map<ActorRef, Integer> removeRandomNeighbors(int max, ActorRef exclude) {
		List<Integer> toRemove = IntStream.range(0, cache.size()).boxed().collect(Collectors.toList());
		Collections.shuffle(toRemove, rand);
		// Limit the numbe of elements to be removed to max + 1 (the potentially
		// excluded element could have been selected)
		toRemove = toRemove.subList(0, Math.min(max + 1, toRemove.size()));

		Map<ActorRef, Integer> removed = new HashMap<>();

		int index = 0;
		Iterator<Entry<ActorRef, Integer>> i = cache.entrySet().iterator();
		while (i.hasNext() && removed.size() < max) {
			Entry<ActorRef, Integer> e = i.next();

			// Remove element with current index from the list of the ones
			// selected to be removed
			// If the current index was not present the entry removal is skipped
			if (!toRemove.remove((Integer) index++))
				continue;

			// Skip if excluded from removal
			if (exclude != null && e.equals(exclude))
				continue;

			removed.put(e.getKey(), e.getValue());
			i.remove();
		}

		return removed;
	}

	public Entry<ActorRef, Integer> removeOldestNeighbor() {
		Entry<ActorRef, Integer> oldest = cache.entrySet().stream().collect(Collectors.maxBy((e1, e2) -> e1.getValue().compareTo(e2.getValue()))).get();
		cache.remove(oldest.getKey());
		return oldest;
	}

	public int size() {
		return cache.size();
	}

	public int maxSize() {
		return MAX_SIZE;
	}

	public Set<ActorRef> getNeighbors() {
		return Collections.unmodifiableSet(cache.keySet());
	}

	@Override
	public String toString() {
		return "NeighborsCache " + cache;
	}
}