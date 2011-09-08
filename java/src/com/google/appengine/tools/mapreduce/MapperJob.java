// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.mapreduce.inputs.DatastoreInput;

/**
 * A builder for mapper jobs and their execution. Example:
 * <code>
 *       String jobId = MapperJob
 *         .withMapper(new MyMapper())
 *         .withInput(new DatastoreInput("MyEntityKind"))
 *         .run();
 * </code>
 *
 */
public final class MapperJob<K, V, OK, OV> {
// ------------------------------ FIELDS ------------------------------

  private String baseUrl = "/mapreduce/";
  private final MapperJobSpecification<K, V, OK, OV> specification = new MapperJobSpecification<K, V, OK, OV>();

// -------------------------- STATIC METHODS --------------------------

  /**
   * Create new mapper job for a given mapper.
   */
  public static <K, V, OK, OV> MapperJob<K, V, OK, OV> withMapper(Mapper<K, V, OK, OV> mapper) {
    MapperJob<K, V, OK, OV> job = new MapperJob<K, V, OK, OV>();
    job.specification.setMapper(mapper);
    return job;
  }

  /**
   * Create new mapper job with {@link DatastoreInput} over specified entity kind.
   */
  public static <OK, OV> MapperJob<Key, Entity, OK, OV> onEntity(String entityKind,
      Mapper<Key, Entity, OK, OV> mapper) {
    return withMapper(mapper).withInput(new DatastoreInput(entityKind));
  }

// --------------------------- CONSTRUCTORS ---------------------------

  private MapperJob() {
  }

// -------------------------- OTHER METHODS --------------------------

  /**
   * Create a task on specified queue with given url upon job completion.
   */
  public MapperJob<K, V, OK, OV> callUrlWhenDone(String url, String queueName) {
    specification.setDoneCallbackUrl(url);
    specification.setDoneCallbackQueueName(queueName);
    return this;
  }

  /**
   * Execute mapper job on specified task queue.
   */
  public MapperJob<K, V, OK, OV> onQueue(String queueName) {
    specification.setWorkerQueueName(queueName);
    specification.setControllerQueueName(queueName);
    return this;
  }

  /**
   * Start the job.
   * @return job id.
   */
  public String run() {
    return MapReduceFactory.getMapReduce().runMapper(baseUrl, specification);
  }

  /**
   * Use mapper library with specified base url.
   */
  public MapperJob<K, V, OK, OV> withBaseUrl(String baseUrl) {
    this.baseUrl = baseUrl;
    return this;
  }

  /**
   * Run mapper over supplied input.
   */
  public MapperJob<K, V, OK, OV> withInput(Input<K, V> input) {
    specification.setInput(input);
    return this;
  }

  /**
   * Limit overall mapper processing rate to specified number of keys per seconds.
   */
  public MapperJob<K, V, OK, OV> withInputProcessingRate(int processingRate) {
    specification.setInputProcessingRate(processingRate);
    return this;
  }

  /**
   * Set mapper job name.
   */
  public MapperJob<K, V, OK, OV> withName(String jobName) {
    specification.setJobName(jobName);
    return this;
  }

  /**
   * Store intermediate results in the supplied output.
   */
  public MapperJob<K, V, OK, OV> withOutput(Output<OK, OV> output) {
    specification.setOutput(output);
    return this;
  }
}
