// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.impl.handlers;

import static com.google.appengine.tools.mapreduce.impl.util.TaskQueueUtil.formatTaskName;

import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Builder;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.google.appengine.tools.mapreduce.MapperJobSpecification;
import com.google.appengine.tools.mapreduce.impl.MapperStateEntity;

import javax.servlet.http.HttpServletRequest;

/**
 * Request context for {@link ControllerHandler}.
 *
 */
final class ControllerHandlerContext<K, V, OK, OV> extends HandlerContext<K, V, OK, OV> {
// ------------------------------ FIELDS ------------------------------

  private final int sliceNumber;

// --------------------------- CONSTRUCTORS ---------------------------

  private ControllerHandlerContext(String jobId, String baseUrl, int sliceNumber) {
    super(jobId, baseUrl);
    this.sliceNumber = sliceNumber;
  }

  private ControllerHandlerContext(String jobId, String baseUrl, int sliceNumber,
      MapperStateEntity<K, V, OK, OV> state,
      MapperJobSpecification<K, V, OK, OV> specification) {
    super(jobId, baseUrl, specification, state);
    this.sliceNumber = sliceNumber;
  }

// -------------------------- INSTANCE METHODS --------------------------

  public TaskOptions createTaskOptionsForNextSlice() {
    int nextSliceNumber = sliceNumber + 1;
    String taskName = formatTaskName("controller-%s-%d", getJobId(), nextSliceNumber);
    return Builder.withMethod(Method.POST)
        .url(getBaseUrl() + MapReduceServletImpl.CONTROLLER_PATH)
        .param(JOB_ID_PARAMETER_NAME, getJobId())
        .param(SLICE_NUMBER_PARAMETER_NAME, String.valueOf(nextSliceNumber))
        .countdownMillis(ControllerHandler.CONTROLLER_DELAY_MS)
        .taskName(taskName);
  }

// -------------------------- STATIC METHODS --------------------------

  static <K, V, OK, OV> ControllerHandlerContext<K, V, OK, OV> createForNewJob(String jobID,
      MapperJobSpecification<K, V, OK, OV> specification, String baseUrl) {
    MapperStateEntity<K, V, OK, OV> state = MapperStateEntity.createForNewJob(
        specification.getJobName(), jobID, System.currentTimeMillis());
    state.setSpecification(specification);
    return new ControllerHandlerContext<K, V, OK, OV>(jobID, baseUrl, -1, state, specification);
  }

  static <K, V, OK, OV> ControllerHandlerContext<K, V, OK, OV> createFromRequest(
      HttpServletRequest request) {
    String jobId = request.getParameter(JOB_ID_PARAMETER_NAME);
    int sliceNumber = Integer.parseInt(
        request.getParameter(SLICE_NUMBER_PARAMETER_NAME));
    return new ControllerHandlerContext<K, V, OK, OV>(jobId, MapReduceServletImpl.getBase(request), sliceNumber);
  }
}
