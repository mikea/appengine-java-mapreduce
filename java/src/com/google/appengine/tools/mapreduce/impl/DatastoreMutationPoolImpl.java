/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityTranslator;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.mapreduce.DatastoreMutationPool;

import java.util.ArrayList;
import java.util.Collection;

/**
 * DatastoreMutationPoolImpl allows you to pool datastore operations such that
 * they are applied in batches requiring fewer datastore API calls. Mutations
 * are accumulated until they reach a count limit on the number of unflushed
 * mutations, until they reach a size limit on the byte count of unflushed
 * mutations, or until a manual flush is requested.
 *
 */
public class DatastoreMutationPoolImpl extends DatastoreMutationPool {
// --------------------------- STATIC FIELDS ---------------------------

  /**
   * Default number of operations to batch before automatically flushing
   */
  public static final int DEFAULT_COUNT_LIMIT = 100;

  /**
   * Default size (in bytes) of operations to batch before automatically
   * flushing.
   *
   * <p>The current value is 256 KB.
   */
  public static final int DEFAULT_SIZE_LIMIT = 1 << 18;

// ------------------------------ FIELDS ------------------------------

  private final DatastoreService ds;

  private final int countLimit;
  private final int sizeLimit;

  private final Collection<Entity> puts = new ArrayList<Entity>();
  private int putsSize = 0;

  private final Collection<Key> deletes = new ArrayList<Key>();
  private int deletesSize = 0;

// --------------------------- CONSTRUCTORS ---------------------------

  @SuppressWarnings("UnusedDeclaration")
  public DatastoreMutationPoolImpl() {
    this(DatastoreServiceFactory.getDatastoreService(), DEFAULT_COUNT_LIMIT, DEFAULT_SIZE_LIMIT);
  }

  DatastoreMutationPoolImpl(DatastoreService ds, int countLimit, int sizeLimit) {
    this.ds = ds;
    this.countLimit = countLimit;
    this.sizeLimit = sizeLimit;
  }

// ------------------------ IMPLEMENTING METHODS ------------------------

  @Override
  public void delete(Key key) {
    // This is probably a serious overestimation, but I can't see a good
    // way to find the size in the public API.
    int deleteSize = KeyFactory.keyToString(key).length();

    // Do this before the add so that we guarantee that size is never > sizeLimit
    if (deletesSize + deleteSize >= sizeLimit) {
      flushDeletes();
    }

    deletesSize += deleteSize;
    deletes.add(key);

    if (deletes.size() >= countLimit) {
      flushDeletes();
    }
  }

  @Override
  public void flush() {
    if (!puts.isEmpty()) {
      flushPuts();
    }

    if (!deletes.isEmpty()) {
      flushDeletes();
    }
  }

  @Override
  public void put(Entity entity) {
    int putSize = EntityTranslator.convertToPb(entity).getSerializedSize();

    // Do this before the add so that we guarantee that size is never > sizeLimit
    if (putsSize + putSize >= sizeLimit) {
      flushPuts();
    }

    putsSize += putSize;
    puts.add(entity);

    if (puts.size() >= countLimit) {
      flushPuts();
    }
  }

// -------------------------- INSTANCE METHODS --------------------------

  private void flushDeletes() {
    ds.delete(deletes);
    deletes.clear();
    deletesSize = 0;
  }

  private void flushPuts() {
    ds.put(puts);
    puts.clear();
    putsSize = 0;
  }
}
