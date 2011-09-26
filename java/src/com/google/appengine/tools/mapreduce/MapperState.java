// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

/**
 */
public interface MapperState {

  Status getStatus();
  Counters getCounters();
}
