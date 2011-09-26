// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

/**
 * A controller clas for the MapReduce framework. It's used for controlling executions and querying
 * statuses. Instances are obtained through {@link MapReduceFactory}
 *
 */
public interface MapReduce {

  /**
   * Start new mapper job.
   * @param baseUrl base url of mapper framework. Usually it's "/mapreduce/"
   * @param specification mapper job specification
   * @return mapper job id
   */
  <K, V, OK, OV> String runMapper(String baseUrl, MapperJobSpecification<K, V, OK, OV> specification);

  MapperState getMapperState(String jobId);
}
