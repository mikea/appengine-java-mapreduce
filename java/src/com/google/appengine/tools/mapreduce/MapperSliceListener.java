// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

/**
 * A class implementing {@link Mapper} may also implement this interface to receive callbacks
 * at slice initialization and termination time.
 *
 * <p>Slice is a chunk of the input processed inside a single task queue. The framework processes
 * a whole slice without any serialization/deserialization process in the middle. A mapper can
 * perform necessary initialization/cleanup to speed up serialization process.</p>
 *
 */
public interface MapperSliceListener<K, V, OK, OV> {
  /**
   * Called every time new slice is started.
   * @param context job context
   */
  void initializeSlice(MapperContext<K, V, OK, OV> context);

  /**
   * Called every time slice is terminated.
   * @param context job context
   */
  void terminateSlice(MapperContext<K, V, OK, OV> context);
}
