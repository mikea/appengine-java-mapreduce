// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.handlers;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.tools.mapreduce.impl.MapperStateEntity;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * UI Status view logic handler.
 *
 */
final class StatusHandler {
// --------------------------- STATIC FIELDS ---------------------------

  public static final int DEFAULT_JOBS_PER_PAGE_COUNT = 50;
  // Command paths
  public static final String LIST_JOBS_PATH = "list_jobs";
  public static final String LIST_CONFIGS_PATH = "list_configs";
  public static final String CLEANUP_JOB_PATH = "cleanup_job";
  public static final String ABORT_JOB_PATH = "abort_job";
  public static final String GET_JOB_DETAIL_PATH = "get_job_detail";
  public static final String START_JOB_PATH = "start_job";

// --------------------------- CONSTRUCTORS ---------------------------

  private StatusHandler() {
  }

// -------------------------- STATIC METHODS --------------------------

  /**
   * Handle the cleanup_job AJAX command.
   */
  private static JSONObject handleCleanupJob(String jobId) throws JSONException {
    JSONObject retValue = new JSONObject();
    MapperStateEntity<?, ?, ?, ?> state = MapperStateEntity.getMapReduceStateFromJobID(jobId);
    if (state == null) {
      retValue.put("status", "Couldn't find requested job.");
    } else {
      state.delete();
      retValue.put("status", "Successfully deleted requested job.");
    }
    return retValue;
  }

  /**
   * Handles all status page commands.
   */
  static void handleCommand(
      String command, HttpServletRequest request, HttpServletResponse response) {
    response.setContentType("application/json");
    boolean isPost = "POST".equals(request.getMethod());
    JSONObject retValue;
    try {
      if (command.equals(LIST_CONFIGS_PATH) && !isPost) {
        retValue = handleListConfigs();
      } else if (command.equals(LIST_JOBS_PATH) && !isPost) {
        retValue = handleListJobs(request);
      } else if (command.equals(CLEANUP_JOB_PATH) && isPost) {
        retValue = handleCleanupJob(request.getParameter("mapreduce_id"));
      } else if (command.equals(ABORT_JOB_PATH) && isPost) {
        retValue = ControllerHandler.handleAbortJob(request.getParameter("mapreduce_id"));
      } else if (command.equals(GET_JOB_DETAIL_PATH) && !isPost) {
        retValue = handleGetJobDetail(request.getParameter("mapreduce_id"));
      } else if (command.equals(START_JOB_PATH) && isPost) {
        retValue = handleStartJob();
      } else {
        response.sendError(404);
        return;
      }
    } catch (Throwable t) {
      MapReduceServletImpl.LOG.log(Level.SEVERE, "Got exception while running command", t);
      try {
        retValue = new JSONObject();
        retValue.put("error_class", t.getClass().getName());
        retValue.put("error_message",
            "Full stack trace is available in the server logs. Message: "
            + t.getMessage());
      } catch (JSONException e) {
        throw new RuntimeException("Couldn't create error JSON object", e);
      }
    }
    try {
      retValue.write(response.getWriter());
      response.getWriter().flush();
    } catch (JSONException e) {
        throw new RuntimeException("Couldn't write command response", e);
    } catch (IOException e) {
      throw new RuntimeException("Couldn't write command response", e);
    }
  }

  /**
   * Handle the get_job_detail AJAX command.
   */
  private static JSONObject handleGetJobDetail(String jobId) {
    MapperStateEntity<?, ?, ?, ?> state = MapperStateEntity.getMapReduceStateFromJobID(jobId);
    if (state == null) {
      throw new IllegalArgumentException("Couldn't find MapReduce for id:" + jobId);
    }
    return state.toJson(true);
  }

  private static JSONObject handleListConfigs() {
    return new JSONObject();
  }

  private static JSONObject handleListJobs(HttpServletRequest request) throws JSONException {
    String cursor = request.getParameter("cursor");
    String countString = request.getParameter("count");
    int count = DEFAULT_JOBS_PER_PAGE_COUNT;
    if (countString != null) {
      count = Integer.parseInt(countString);
    }

    return handleListJobs(cursor, count);
  }

  /**
   * Handle the list_jobs AJAX command.
   */
  private static JSONObject handleListJobs(String cursor, int count) throws JSONException {
    Collection<MapperStateEntity<?, ?, ?, ?>> states = new ArrayList<MapperStateEntity<?, ?, ?, ?>>();
    Cursor newCursor = MapperStateEntity.getMapReduceStates(cursor, count, states);
    JSONArray jobs = new JSONArray();
    for (MapperStateEntity<?, ?, ?, ?> state : states) {
      jobs.put(state.toJson(false));
    }

    JSONObject retValue = new JSONObject();
    retValue.put("jobs", jobs);
    if (newCursor != null) {
      retValue.put("cursor", newCursor.toWebSafeString());
    }

    return retValue;
  }

  private static JSONObject handleStartJob() {
    throw new UnsupportedOperationException("handleStartJob is not implemented.");
  }
}
