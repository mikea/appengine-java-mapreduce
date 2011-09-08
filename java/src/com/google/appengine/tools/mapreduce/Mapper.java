// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import java.io.Serializable;

/**
 * <p>A map function for use in mapreduce computations or in a standalone mapper process. A map
 * function processes a key-value pair and possibly generates a set of intermediate key-value
 * pairs, stored to the output obtained through the context</p>
 *
 * <p>This class is really an interface that might be evolving. In order to avoid breaking
 * users when we change the interface, we made it an abstract class.</p>
 *
 * @param <K> input key type
 * @param <V> input value type
 * @param <OK> output key type
 * @param <OV> output value type
 *
 */
public abstract class Mapper<K, V, OK, OV> implements Serializable {
  private static final long serialVersionUID = 1966174340710715049L;

  /**
   * Process a single key-value pair. Mapper output can be obtained from the context.
   *
   * @param key the key to process
   * @param value the value to process
   * @param context mapper context
   */
  public abstract void map(K key, V value, MapperContext<K, V, OK, OV> context);
}
