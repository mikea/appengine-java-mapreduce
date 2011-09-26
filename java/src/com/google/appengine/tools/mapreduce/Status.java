// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

/**
 * Possible job statuses.
 *
 */
public enum Status {
  /**
   * Job is running.
   */
  ACTIVE,

  /**
   * Job has successfully completed.
   */
  DONE,

  /**
   * Job stopped because of error.
   */
  ERROR,

  /**
   * Job stopped because of user abort request.
   */
  ABORTED
}
