// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Reads data for mapper consumption.
 *
 * <p>An implementation of InputReader should be prepared to be serialized after next() call
 * any time and to continue reading after it was deserialized. The library will always call
 * next() after positive hasNext() result.</p>
 *
 * <p>This class is really an interface that might be evolving. In order to avoid breaking
 * users when we change the interface, we made it an abstract class.</p>
 *
 */
public abstract class InputReader<K, V> implements Iterator<InputReader.KeyValue<K, V>>, Serializable {
  private static final long serialVersionUID = -2687854533615172942L;

  /**
   * @return input reading progress from 0 to 1.
   */
  public abstract double getProgress();

  @Override
  public void remove() {
    throw new UnsupportedOperationException(
        "remove is not implemented in com.google.appengine.tools.mapreduce.InputReader");
  }

  /**
   * Simple key-value pair.
   */
  public static class KeyValue<K, V> {
    public final K key;
    public final V value;

    public KeyValue(K key, V value) {
      this.key = key;
      this.value = value;
    }

    public static <K, V> KeyValue<K, V> of(K k, V v) {
      return new KeyValue<K, V>(k, v);
    }
  }
}
