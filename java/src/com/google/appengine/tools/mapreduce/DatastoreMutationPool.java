// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.mapreduce.impl.DatastoreMutationPoolImpl;

/**
 * A pool that batches multiple datastore operations together to improve their performance.
 *
 * <p>This class is really an interface that might be evolving. In order to avoid breaking
 * users when we change the interface, we made it an abstract class.</p>
 *
 */
public abstract class DatastoreMutationPool extends Pool {

  /**
   * Unique pool key.
   */
  public static final PoolKey<DatastoreMutationPool> KEY =
      new PoolKey<DatastoreMutationPool>(DatastoreMutationPoolImpl.class);

  /**
   * Adds a mutation inserting the given {@code entity}.
   * @param entity to insert (update)
   */
  public abstract void put(Entity entity);

  /**
   * Adds a mutation deleting the entity corresponding to {@code key}.
   * @param key entity key to delete
   */
  public abstract void delete(Key key);
}
