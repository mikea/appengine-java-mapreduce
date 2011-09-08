// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

/**
 * A class implementing {@link Mapper} may also implement this interface to receive callbacks
 * at shard initialization and termination time.
 *
 */
public interface MapperShardListener<K, V, OK, OV> {
  /**
   * Called once for each shard during shard initialization.
   * @param context job context
   */
  void initializeShard(MapperContext<K, V, OK, OV> context);

  /**
   * Called once for each shard during shard termination.
   * @param context job context
   */
  void terminateShard(MapperContext<K, V, OK, OV> context);
}
