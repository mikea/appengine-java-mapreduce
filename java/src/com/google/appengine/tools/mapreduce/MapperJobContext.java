// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

/**
 * Defines context for mapper job as a whole.
 *
 */
public interface MapperJobContext<K, V, OK, OV>  {

  /**
   * @return current job id.
   */
  String getJobId();

  /**
   * @return mapper specification for current job.
   */
  MapperJobSpecification<K, V, OK, OV> getSpecification();
}
