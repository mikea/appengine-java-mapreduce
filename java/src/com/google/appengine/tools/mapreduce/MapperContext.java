// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.mapreduce.Pool.PoolKey;

/**
 * A context for mapper execution. Provides everything that might be needed by a mapper function.
 *
 */
public interface MapperContext<K, V, OK, OV> extends MapperJobContext<K, V, OK, OV> {

  /**
   * @return counters object for doing simple aggregate calculations.
   */
  Counters getCounters();

  /**
   * @return map output to store intermediate data.
   */
  MapperOutput<OK, OV> getOutput();

  /**
   * A pool can be used to batch some operations together to reduce load on the underlying
   * backend. Typical pool is a {@link DatastoreMutationPool}. Pools can be obtained by their
   * unique key.
   *
   * @param key pool's key provided by pool developer.
   * @return an instance of the pool.
   */
  <T extends Pool> T getPool(PoolKey<T> key);

  /**
   * @return the shard number mapper function is executed in.
   */
  int getShardNumber();

  /**
   * Interface for outputting mapper intermediate data.
   * @param <K>
   * @param <V>
   */
  interface MapperOutput<K, V> {
    void emit(K key, V value);
  }
}
