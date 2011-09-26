// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.impl.handlers;

import static com.google.appengine.tools.mapreduce.impl.util.TaskQueueUtil.formatTaskName;

import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Builder;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.google.appengine.tools.mapreduce.impl.ShardStateEntity;

import javax.servlet.http.HttpServletRequest;

/**
 * Request context for {@link WorkerHandler}.
 *
 */
final class WorkerHandlerContext<K, V, OK, OV> extends HandlerContext<K, V, OK, OV> {
// ------------------------------ FIELDS ------------------------------

  private final int shardNumber;
  private final int sliceNumber;
  private ShardStateEntity<K, V, OK, OV> shardState;

// --------------------------- CONSTRUCTORS ---------------------------

  WorkerHandlerContext(String jobId, String baseUrl, int shardNumber, int sliceNumber) {
    super(jobId, baseUrl);
    this.shardNumber = shardNumber;
    this.sliceNumber = sliceNumber;
  }

// --------------------- GETTER / SETTER METHODS ---------------------

  public int getShardNumber() {
    return shardNumber;
  }

  public ShardStateEntity<K, V, OK, OV> getShardState() {
    if (shardState == null) {
      shardState = ShardStateEntity.getShardState(getJobId(), shardNumber);
    }

    return shardState;
  }

  public int getSliceNumber() {
    return sliceNumber;
  }

// -------------------------- INSTANCE METHODS --------------------------

  TaskOptions createTaskOptionsForNextSlice() {
    int nextSliceNumber = sliceNumber + 1;
    String taskName = formatTaskName("worker-%s-%d-%d", getJobId(), shardNumber, nextSliceNumber);
    return Builder.withMethod(Method.POST)
        .url(getBaseUrl() + MapReduceServletImpl.MAPPER_WORKER_PATH)
        .param(JOB_ID_PARAMETER_NAME, getJobId())
        .param(SHARD_NUMBER_PARAMETER_NAME, String.valueOf(shardNumber))
        .param(SLICE_NUMBER_PARAMETER_NAME, String.valueOf(nextSliceNumber))
        .taskName(taskName);
  }

// -------------------------- STATIC METHODS --------------------------

  public static <K, V, OK, OV> WorkerHandlerContext<K, V, OK, OV> createForNewJob(
      String jobId, int shardNumber, String baseUrl) {
    return new WorkerHandlerContext<K, V, OK, OV>(jobId, baseUrl, shardNumber, -1);
  }

  public static <K, V, OK, OV> WorkerHandlerContext<K, V, OK, OV> createFromRequest(
      HttpServletRequest request) {
    String jobId = request.getParameter(JOB_ID_PARAMETER_NAME);
    int shardNumber = Integer.parseInt(request.getParameter(SHARD_NUMBER_PARAMETER_NAME));
    int sliceNumber = Integer.parseInt(request.getParameter(SLICE_NUMBER_PARAMETER_NAME));
    return new WorkerHandlerContext<K, V, OK, OV>(jobId, MapReduceServletImpl.getBase(request), shardNumber, sliceNumber);
  }
}
