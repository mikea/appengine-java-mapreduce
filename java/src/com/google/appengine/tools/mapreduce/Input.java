// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import java.io.Serializable;
import java.util.List;

/**
 * Input is the data source specification for the job. Input simply defines data source, while
 * {@link InputReader} handles reading itself.
 *
 * <p>This class is really an interface that might be evolving. In order to avoid breaking
 * users when we change the interface, we made it an abstract class.</p>
 *
 */
public abstract class Input<K, V> implements Serializable {
  private static final long serialVersionUID = 8796820298129705263L;

  /**
   * Split input into multiple readers. It's Input's responsibility to determine appropriate
   * number of readers to split into. This could be specified by user or algorithmically
   * determined.
   *
   * @return list of readers
   */
  public abstract List<? extends InputReader<K, V>> split(MapperJobContext<K, V, ?, ?> context);
}
