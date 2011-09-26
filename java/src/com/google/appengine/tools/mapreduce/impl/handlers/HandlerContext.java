// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.impl.handlers;

import com.google.appengine.tools.mapreduce.MapperJobSpecification;
import com.google.appengine.tools.mapreduce.impl.MapperStateEntity;

/**
 * Handler context extracts the execution context from http request.
 *
 */
class HandlerContext<K, V, OK, OV> {
// --------------------------- STATIC FIELDS ---------------------------

  static final String JOB_ID_PARAMETER_NAME = "jobID";
  static final String SLICE_NUMBER_PARAMETER_NAME = "sliceNumber";
  static final String SHARD_NUMBER_PARAMETER_NAME = "shardNumber";

// ------------------------------ FIELDS ------------------------------

  private final String jobId;
  private final String baseUrl;
  private MapperJobSpecification<K, V, OK, OV> specification = null;
  private MapperStateEntity<K, V, OK, OV> state = null;

// --------------------------- CONSTRUCTORS ---------------------------

  HandlerContext(String jobId, String baseUrl) {
    this.jobId = jobId;
    this.baseUrl = baseUrl;
  }

  HandlerContext(String jobId, String baseUrl,
      MapperJobSpecification<K, V, OK, OV> specification,
      MapperStateEntity<K, V, OK, OV> state) {
    this.jobId = jobId;
    this.baseUrl = baseUrl;
    this.specification = specification;
    this.state = state;
  }

// --------------------- GETTER / SETTER METHODS ---------------------

  public String getBaseUrl() {
    return baseUrl;
  }

  public String getJobId() {
    return jobId;
  }

  public MapperJobSpecification<K, V, OK, OV> getSpecification() {
    if (specification == null) {
      MapperStateEntity<K, V, OK, OV> mapperState = getState();
      if (mapperState != null) {
        specification = mapperState.getSpecification();
      }
    }
    return specification;
  }

  public MapperStateEntity<K, V, OK, OV> getState() {
    if (state == null) {
      state = MapperStateEntity.getMapReduceStateFromJobID(jobId);
    }
    return state;
  }
}
