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

import static com.google.appengine.api.datastore.DatastoreServiceFactory.getDatastoreService;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.Status;
import com.google.appengine.tools.mapreduce.impl.util.Clock;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.appengine.tools.mapreduce.impl.util.SystemClock;
import com.google.common.base.Preconditions;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Thin wrapper around a shard state entity in the datastore.
 *
 */
public class ShardStateEntity<K, V, OK, OV> {
// --------------------------- STATIC FIELDS ---------------------------

  private static final String ENTITY_KIND = "ShardStateEntity";

  private static final String COUNTERS_MAP_PROPERTY = "countersMap";
  private static final String INPUT_READER_PROPERTY = "inputReader";
  private static final String JOB_ID_PROPERTY = "jobId";
  private static final String STATUS_PROPERTY = "status";
  private static final String UPDATE_TIMESTAMP_PROPERTY = "updateTimestamp";

// ------------------------------ FIELDS ------------------------------

  // The shard state entity
  private final Entity entity;

  private final Clock clock = new SystemClock();
  private final Counters counters;
  private final InputReader<K, V> inputReader;

// --------------------------- CONSTRUCTORS ---------------------------

  /**
   * Creates the ShardStateEntity from its corresponding entity.
   */
  private ShardStateEntity(Entity entity) {
    this.entity = entity;
    counters = (Counters) SerializationUtil.deserializeFromByteArray(
        ((Blob) entity.getProperty(COUNTERS_MAP_PROPERTY)).getBytes());
    inputReader =  (InputReader<K, V>) SerializationUtil.deserializeFromByteArray(
        ((Blob) entity.getProperty(INPUT_READER_PROPERTY)).getBytes());
  }

  /**
   * Creates new shard state for new job.
   */
  private ShardStateEntity(String jobId, int shardNumber, InputReader<K, V> reader) {
    entity = new Entity(createKey(jobId, shardNumber));
    entity.setProperty(JOB_ID_PROPERTY, jobId);

    counters = new CountersImpl();
    inputReader = reader;
    setStatus(Status.ACTIVE);
  }

// --------------------- GETTER / SETTER METHODS ---------------------

  public Counters getCounters() {
    return counters;
  }

  public InputReader<K, V> getInputReader() {
    return inputReader;
  }

  public int getShardNumber() {
    String name = entity.getKey().getName();
    return Integer.valueOf(name.substring(name.indexOf('-') + 1));
  }

  public Status getStatus() {
    return Status.valueOf((String) entity.getProperty(STATUS_PROPERTY));
  }

  public void setStatus(Status status) {
    entity.setProperty(STATUS_PROPERTY, status.toString());
  }

  /**
   * Get the status string from the shard state. This is a user-defined message, intended to inform
   * a human of the status of the current shard.
   *
   * @return the status string
   */
  String getStatusString() {
    return (String) entity.getProperty(STATUS_PROPERTY);
  }

  /**
   * Returns the update timestamp of this shard in milliseconds since the epoch.
   */
  long getUpdateTimestamp() {
    return (Long) entity.getProperty(UPDATE_TIMESTAMP_PROPERTY);
  }

  /**
   * Set the time the state was last updated in milliseconds since the epoch.
   */
  void setUpdateTimestamp(long timestamp) {
    entity.setProperty(UPDATE_TIMESTAMP_PROPERTY, timestamp);
  }

// -------------------------- INSTANCE METHODS --------------------------

  /**
   * Persists this to the datastore.
   */
  public void persist() {
    entity.setUnindexedProperty(COUNTERS_MAP_PROPERTY,
        new Blob(SerializationUtil.serializeToByteArray(counters)));
    entity.setUnindexedProperty(INPUT_READER_PROPERTY,
        new Blob(SerializationUtil.serializeToByteArray(inputReader)));

    checkComplete();
    setUpdateTimestamp(clock.currentTimeMillis());
    getDatastoreService().put(entity);
  }

  /**
   * Create JSON object from this object.
   */
  public JSONObject toJson() {
    JSONObject shardObject = new JSONObject();
    try {
      shardObject.put("shard_number", getShardNumber());
      shardObject.put("active", getStatus() == Status.ACTIVE);
      shardObject.put("shard_description", getStatusString());
      shardObject.put("updated_timestamp_ms", getUpdateTimestamp());
      shardObject.put("result_status", getStatusString());
    } catch (JSONException e) {
      throw new RuntimeException("Hard coded string is null", e);
    }
    return shardObject;
  }

  private void checkComplete() {
    Preconditions.checkNotNull(
        entity.getProperty(INPUT_READER_PROPERTY),
        "Input reader must be set.");
    Preconditions.checkNotNull(
        entity.getProperty(COUNTERS_MAP_PROPERTY),
        "Counters map must be set.");
  }

// -------------------------- STATIC METHODS --------------------------

  /**
   * Creates a shard state that's active but hasn't made any progress as of yet.
   *
   * @return the initialized shard state
   */
  public static <K, V, OK, OV> ShardStateEntity<K, V, OK, OV> createForNewJob(
      String jobId, int shardNumber, InputReader<K, V> reader) {
    return new ShardStateEntity<K, V, OK, OV>(jobId, shardNumber, reader);
  }

  public static <K, V, OK, OV> void deleteAllShards(
      Iterable<ShardStateEntity<K, V, OK, OV>> shardStates) {
    Collection<Key> keys = new ArrayList<Key>();
    for (ShardStateEntity<K, V, OK, OV> shardState : shardStates) {
      keys.add(shardState.entity.getKey());
    }
    getDatastoreService().delete(keys);
  }

  /**
   * Gets the ShardStateEntity corresponding to the given TaskID.
   *
   * @return the shard state corresponding to the provided key
   */
  public static <K, V, OK, OV> ShardStateEntity<K, V, OK, OV> getShardState(String jobId,
      int shardNumber) {
    Key key = createKey(jobId, shardNumber);
    Entity entity;
    try {
      entity = getDatastoreService().get(key);
    } catch (EntityNotFoundException ignored) {
      return null;
    }
    return new ShardStateEntity<K, V, OK, OV>(entity);
  }

  /**
   * Gets all shard states corresponding to a particular Job ID
   */
  public static <K, V, OK, OV> List<ShardStateEntity<K, V, OK, OV>> getShardStates(
      MapperStateEntity<K, V, OK, OV> mapperState) {
    Collection<Key> keys = new ArrayList<Key>();
    for (int i = 0; i < mapperState.getShardCount(); ++i) {
      keys.add(createKey(mapperState.getJobID(), i));
    }

    Map<Key, Entity> map = getDatastoreService().get(keys);
    List<ShardStateEntity<K, V, OK, OV>> shardStates = new ArrayList<ShardStateEntity<K, V, OK, OV>>(
        map.size());
    for (Key key : keys) {
      Entity entity = map.get(key);
      if (entity != null) {
        ShardStateEntity<K, V, OK, OV> shardState = new ShardStateEntity<K, V, OK, OV>(entity);
        shardStates.add(shardState);
      }
    }
    return shardStates;
  }

  /**
   * Return all shard ids with status == ACTIVE.
   */
  public static <K, V, OK, OV> List<Integer> selectActiveShards(
      Iterable<ShardStateEntity<K, V, OK, OV>> shardStates) {
    List<Integer> activeShardStates = new ArrayList<Integer>();
    for (ShardStateEntity<K, V, OK, OV> shardState : shardStates) {
      if (shardState.getStatus() == Status.ACTIVE) {
        activeShardStates.add(shardState.getShardNumber());
      }
    }
    return activeShardStates;
  }

  private static Key createKey(String jobId, int shardNumber) {
    return KeyFactory.createKey(ENTITY_KIND, String.format("%s-%d", jobId, shardNumber));
  }
}
