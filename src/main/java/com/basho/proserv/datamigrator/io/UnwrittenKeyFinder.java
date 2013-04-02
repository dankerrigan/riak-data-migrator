package com.basho.proserv.datamigrator.io;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class UnwrittenKeyFinder implements Iterable<Key> {
	private Set<Key> referenceKeySet = new HashSet<Key>();
	private final Iterable<Key> referenceKeys;
	private final Iterable<Key> compareKeys;
	
	public UnwrittenKeyFinder(Iterable<Key> referenceKeys, Iterable<Key> compareKeys) {
		this.referenceKeys = referenceKeys;
		this.compareKeys = compareKeys;
		
		this.loadKeys(this.referenceKeys, this.referenceKeySet);
	}
	
	
	public void loadReferenceKeys(Iterable<Key> keys) {
		loadKeys(keys, referenceKeySet);
	}
		
	public boolean keyExists(Key key) {
		if (referenceKeySet.size() == 0) {
			throw new IllegalArgumentException("Reference Keys have not been loaded");
		}
		
		return this.referenceKeySet.contains(key);
	}

	
	public Iterator<Key> iterator() {
		return new UnwrittenKeyIterator(this);
	}
	
	private void loadKeys(Iterable<Key> keys, Set<Key> toLoad) {
		toLoad.clear();
		for (Key key : keys) {
			toLoad.add(key);
		}
	}
	
	private class UnwrittenKeyIterator implements Iterator<Key> {
		private final UnwrittenKeyFinder comparator;
		private final Iterator<Key> keyIterator;
		
		private Key nextKey = null;
		
		public UnwrittenKeyIterator(UnwrittenKeyFinder comparator) {
			this.comparator = comparator;
			this.keyIterator = comparator.compareKeys.iterator();
			this.setNextKey();
		}
		
		
		public boolean hasNext() {
			return nextKey == null;
		}

		
		public Key next() {
			Key currentKey = this.nextKey;
			this.setNextKey();
			
			return currentKey;
		}
		
		private void setNextKey() {
			try {
				while (!Thread.interrupted()) {
					Key key = keyIterator.next();
					if (!this.comparator.keyExists(key)) {
						nextKey = key;
						break;
					}
				}
			} catch (NoSuchElementException e) {
				nextKey = null;
			}
		}

		
		public void remove() {
			// TODO Auto-generated method stub
			
		}
	}
	
}
