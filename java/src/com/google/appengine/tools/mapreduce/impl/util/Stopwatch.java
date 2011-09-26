// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.impl.util;

/**
 */
public class Stopwatch {
// ------------------------------ FIELDS ------------------------------

  private long totalTime;
  private long start = -1;

// --------------------- GETTER / SETTER METHODS ---------------------

  public long getTimeMillis() {
    return totalTime / 1000000L;
  }

// -------------------------- INSTANCE METHODS --------------------------

  public void start() {
    if (start >= 0) {
      throw new IllegalStateException("Stopwatch is already running");
    }
    start = System.nanoTime();
  }

  public void stop() {
    if (start < 0) {
      throw new IllegalStateException("Stopwatch is not running");
    }

    totalTime += System.nanoTime() - start;
    start = -1;
  }
}
