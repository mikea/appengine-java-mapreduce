// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.inputs;

import static com.google.appengine.api.datastore.DatastoreServiceFactory.getDatastoreService;
import static com.google.appengine.api.datastore.Entity.KEY_RESERVED_PROPERTY;
import static com.google.appengine.api.datastore.FetchOptions.Builder.withChunkSize;
import static com.google.appengine.api.datastore.Query.FilterOperator.GREATER_THAN;
import static com.google.appengine.api.datastore.Query.FilterOperator.GREATER_THAN_OR_EQUAL;
import static com.google.appengine.api.datastore.Query.FilterOperator.LESS_THAN;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.base.Preconditions;

/**
 */
class DatastoreInputReader extends InputReader<Key, Entity> {
// --------------------------- STATIC FIELDS ---------------------------

private static final long serialVersionUID = -2164845668646485549L;

// ------------------------------ FIELDS ------------------------------

  private final String entityKind;
  private final Key startKey;
  private final Key endKey;
  private final int batchSize = 50;
  private Key currentKey;

  private transient Entity currentValue = null;
  private transient QueryResultIterator<Entity> iterator;

// --------------------------- CONSTRUCTORS ---------------------------

  DatastoreInputReader(String entityKind, Key startKey, Key endKey) {
    this.entityKind = entityKind;
    this.startKey = startKey;
    this.endKey = endKey;
  }

// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface Iterator ---------------------

  @Override
  public boolean hasNext() {
    if (iterator == null) {
      createIterator();
    }

    if (!iterator.hasNext()) {
      return false;
    }

    Entity entity = iterator.next();
    currentKey = entity.getKey();
    currentValue = entity;

    return true;
  }

  @Override
  public KeyValue<Key, Entity> next() {
    return KeyValue.of(currentKey, currentValue);
  }

// ------------------------ IMPLEMENTING METHODS ------------------------

  @Override
  public double getProgress() {
    return 0.0;
  }

// --------------------- GETTER / SETTER METHODS ---------------------

  Key getEndKey() {
    return endKey;
  }

  Key getStartKey() {
    return startKey;
  }

// -------------------------- INSTANCE METHODS --------------------------

  private void createIterator() {
    Preconditions.checkState(iterator == null);

    Query q = new Query(entityKind);

    if (currentKey == null) {
      if (startKey != null) {
        q.addFilter(KEY_RESERVED_PROPERTY, GREATER_THAN_OR_EQUAL, startKey);
      }
    } else {
      q.addFilter(KEY_RESERVED_PROPERTY, GREATER_THAN, currentKey);
    }

    if (endKey != null) {
      q.addFilter(KEY_RESERVED_PROPERTY, LESS_THAN, endKey);
    }

    q.addSort(KEY_RESERVED_PROPERTY);

    iterator = getDatastoreService().prepare(q).asQueryResultIterator(withChunkSize(batchSize));
  }
}
