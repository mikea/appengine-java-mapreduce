// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import java.io.Serializable;

/**
 * Specification for a mapper job. Contains all configuration necessary to start a job.
 *
 */
public final class MapperJobSpecification<K, V, OK, OV> implements Serializable {
// ------------------------------ FIELDS ------------------------------

  private static final long serialVersionUID = 5177328196594755342L;

  private String jobName = "mapper";
  private Mapper<K, V, OK, OV> mapper;
  private Input<K, V> input;
  private Output<OK, OV> output;

  private String controllerQueueName = "default";
  private String workerQueueName = "default";
  private String doneCallbackUrl = null;
  private String doneCallbackQueueName = "default";
  private int inputProcessingRate = 1000;

// --------------------------- CONSTRUCTORS ---------------------------

  public MapperJobSpecification() {
  }

  public MapperJobSpecification(Mapper<K, V, OK, OV> mapper, Input<K, V> input,
      Output<OK, OV> output) {
    this.mapper = mapper;
    this.input = input;
    this.output = output;
  }

// --------------------- GETTER / SETTER METHODS ---------------------

  public String getControllerQueueName() {
    return controllerQueueName;
  }

  public void setControllerQueueName(String controllerQueueName) {
    this.controllerQueueName = controllerQueueName;
  }

  public String getDoneCallbackQueueName() {
    return doneCallbackQueueName;
  }

  public void setDoneCallbackQueueName(String doneCallbackQueueName) {
    this.doneCallbackQueueName = doneCallbackQueueName;
  }

  public String getDoneCallbackUrl() {
    return doneCallbackUrl;
  }

  public void setDoneCallbackUrl(String url) {
    doneCallbackUrl = url;
  }

  public Input<K,V> getInput() {
    return input;
  }

  public void setInput(Input<K, V> input) {
    this.input = input;
  }

  public int getInputProcessingRate() {
    return inputProcessingRate;
  }

  public void setInputProcessingRate(int inputProcessingRate) {
    this.inputProcessingRate = inputProcessingRate;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public Mapper<K,V,OK,OV> getMapper() {
    return mapper;
  }

  public void setMapper(Mapper<K, V, OK, OV> mapper) {
    this.mapper = mapper;
  }

  public Output<OK, OV> getOutput() {
    return output;
  }

  public void setOutput(Output<OK, OV> output) {
    this.output = output;
  }

  public String getWorkerQueueName() {
    return workerQueueName;
  }

  public void setWorkerQueueName(String workerQueueName) {
    this.workerQueueName = workerQueueName;
  }

// -------------------------- OTHER METHODS --------------------------

  public boolean hasDoneCallback() {
    return doneCallbackUrl != null;
  }
}
