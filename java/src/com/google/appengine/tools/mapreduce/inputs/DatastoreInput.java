// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.inputs;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.MapperJobContext;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 */
public class DatastoreInput extends Input<Key, Entity> {
// --------------------------- STATIC FIELDS ---------------------------

  private static final Logger logger = Logger.getLogger(DatastoreInput.class.getName());
  private static final String SCATTER_RESERVED_PROPERTY = "__scatter__";
  private static final int SCATTER_OVERSAMPLE_FACTOR = 32;
  private static final long serialVersionUID = -3939543473076385308L;

// ------------------------------ FIELDS ------------------------------

  private final String entityKind;
  private final int shardCount;

// --------------------------- CONSTRUCTORS ---------------------------

  public DatastoreInput(String entityKind) {
    this(entityKind, 4);
  }

  public DatastoreInput(String entityKind, int shardCount) {
    this.entityKind = entityKind;
    this.shardCount = shardCount;
  }

// ------------------------ IMPLEMENTING METHODS ------------------------

  @Override
  public List<? extends InputReader<Key, Entity>> split(
      MapperJobContext<Key, Entity, ?, ?> context) {
    Preconditions.checkNotNull(entityKind);
    logger.info("Getting input splits for: " + entityKind);

    DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();
    Key startKey = getStartKey(entityKind, datastoreService);
    if (startKey == null) {
      return Collections.emptyList();
    }

    Key lastKey = startKey;

    List<DatastoreInputReader> result = new ArrayList<DatastoreInputReader>();
    for (Key currentKey : chooseSplitPoints(datastoreService)) {
      DatastoreInputReader source = new DatastoreInputReader(entityKind, lastKey, currentKey);
      result.add(source);
      logger.info(
          String.format("Added DatastoreInputSplit %s %s %s", source, lastKey, currentKey));
      lastKey = currentKey;
    }

    // Add in the final split. null is special cased so this split contains
    // [lastKey, Infinity).
    result.add(new DatastoreInputReader(entityKind, lastKey, null));

    return result;
  }

// -------------------------- INSTANCE METHODS --------------------------

  private Collection<Key> chooseSplitPoints(DatastoreService datastoreService) {
    int desiredScatterResultCount = shardCount * SCATTER_OVERSAMPLE_FACTOR;
    Query scatter = new Query(entityKind)
        .addSort(SCATTER_RESERVED_PROPERTY)
        .setKeysOnly();
    List<Entity> scatterList = datastoreService.prepare(scatter).asList(
        withLimit(desiredScatterResultCount));
    Collections.sort(scatterList, new Comparator<Entity>() {
      @Override
      public int compare(Entity o1, Entity o2) {
        return o1.getKey().compareTo(o2.getKey());
      }
    });

    Collection<Key> splitKeys = new ArrayList<Key>(shardCount);
    // Possibly use a lower oversampling factor if there aren't enough scatter
    // property-containing entities to fill out the list.
    int usedOversampleFactor = Math.max(1, scatterList.size() / shardCount);
    logger.info("Requested " + desiredScatterResultCount + " scatter entities. Got "
        + scatterList.size() + " so using oversample factor " + usedOversampleFactor);
    // We expect the points to be uniformly randomly distributed. So we
    // act like the first point is the start key (which we alread know) and
    // omit it. This converges on correct as the number of samples goes
    // to infinity.
    for (int i = 1; i < shardCount; i++) {
      // This can happen if we don't have as many scatter properties as we want.
      if (i * usedOversampleFactor >= scatterList.size()) {
        break;
      }
      splitKeys.add(scatterList.get(i * usedOversampleFactor).getKey());
    }
    return splitKeys;
  }

// -------------------------- STATIC METHODS --------------------------

  private static Key getStartKey(String entityKind, DatastoreService datastoreService) {
    Query ascending = new Query(entityKind)
        .addSort(Entity.KEY_RESERVED_PROPERTY)
        .setKeysOnly();
    Iterator<Entity> ascendingIt = datastoreService.prepare(ascending).asIterator(withLimit(1));
    if (!ascendingIt.hasNext()) {
      return null;
    }
    return ascendingIt.next().getKey();
  }
}
