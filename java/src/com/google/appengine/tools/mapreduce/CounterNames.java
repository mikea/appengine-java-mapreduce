// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

/**
 * Built-in counter names.
 *
 */
public final class CounterNames {
  /**
   * Number of times mapper function was called.
   */
  public static final String MAPPER_CALLS = "mapper-calls";

  /**
   * Total time in milliseconds spent in mapper function.
   */
  public static final String MAPPER_WALLTIME_MSEC = "mapper-walltime-msec";

  /**
   * Total number of bytes written to the output.
   */
  public static final String IO_WRITE_BYTES = "io-write-bytes";

  /**
   * Total time in milliseconds spent writing.
   */
  public static final String IO_WRITE_MSEC = "io-write-msec";

  /**
   * Total number of bytes read.
   */
  public static final String IO_READ_BYTES = "io-read-bytes";

  /**
   * Total time in milliseconds spent reading.
   */
  public static final String IO_READ_MSEC = "io-read-msec";

  private CounterNames() {}
}
