// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.MapReduce;
import com.google.appengine.tools.mapreduce.MapperJobSpecification;
import com.google.appengine.tools.mapreduce.MapperState;
import com.google.appengine.tools.mapreduce.impl.handlers.ControllerHandler;

/**
 */
public class MapReduceImpl implements MapReduce {
// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface MapReduce ---------------------

  @Override
  public <K, V, OK, OV> String runMapper(String baseUrl,
      MapperJobSpecification<K, V, OK, OV> specification) {
    return ControllerHandler.handleStart(specification, baseUrl);
  }

  @Override
  public MapperState getMapperState(String jobId) {
    return MapperStateEntity.getMapReduceStateFromJobID(jobId);
  }
}
