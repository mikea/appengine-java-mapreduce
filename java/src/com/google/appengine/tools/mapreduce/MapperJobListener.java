// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

/**
 * A class implementing {@link Mapper} may also implement this interface to receive callbacks
 * at job initialization and termination time.
 *
 */
public interface MapperJobListener<K, V, OK, OV> {

  /**
   * Called once during job initialization process.
   * @param context job context
   */
  void initializeJob(MapperJobContext<K, V, OK, OV> context);

  /**
   * Called once during job shut down proces.
   * @param context job context
   */
  void terminateJob(MapperJobContext<K, V, OK, OV> context);
}
