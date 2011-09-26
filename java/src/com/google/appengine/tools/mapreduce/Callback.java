// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import static com.google.appengine.tools.mapreduce.impl.util.TaskQueueUtil.normalizeTaskName;

import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskAlreadyExistsException;
import com.google.appengine.api.taskqueue.TaskOptions.Builder;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.google.appengine.tools.mapreduce.impl.handlers.MapReduceServletImpl;

import java.io.Serializable;

/**
 * Describes a task-queue callback. The library will queue a task when event of interest occurs.
 *
 */
public class Callback implements Serializable{
// --------------------------- STATIC FIELDS ---------------------------

  private static final long serialVersionUID = -4850905607541756222L;

// ------------------------------ FIELDS ------------------------------

  private String url;
  private Method method = Method.POST;
  private String queue = "default";

// --------------------------- CONSTRUCTORS ---------------------------

  public Callback(String url) {
    this.url = url;
  }

// --------------------- GETTER / SETTER METHODS ---------------------

  public Method getMethod() {
    return method;
  }

  public void setMethod(Method method) {
    this.method = method;
  }

  public String getQueue() {
    return queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

// -------------------------- INSTANCE METHODS --------------------------

  public void schedule(String taskName, String jobId) {
    String normalizedTaskName = normalizeTaskName(taskName);
    try {
      QueueFactory.getQueue(queue).add(Builder
          .withMethod(method)
          .url(url)
          .param("job_id", jobId)
          .taskName(normalizedTaskName));
    } catch (TaskAlreadyExistsException ignored) {
      MapReduceServletImpl.LOG.warning("Done callback task " + normalizedTaskName + " already exists.");
    }
  }
}
