// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.mapreduce.impl.MapReduceImpl;

/**
 * Factory class for {@link MapReduce}.
 *
 */
public final class MapReduceFactory {
  private MapReduceFactory() {
  }

  /**
   * @return mapreduce instance.
   */
  public static MapReduce getMapReduce() {
    return new MapReduceImpl();
  }
}
