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
import static com.google.appengine.api.datastore.FetchOptions.Builder.withPrefetchSize;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.tools.mapreduce.Counter;
import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.MapperJobSpecification;
import com.google.appengine.tools.mapreduce.MapperState;
import com.google.appengine.tools.mapreduce.Status;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.common.base.Preconditions;

import com.googlecode.charts4j.AxisLabelsFactory;
import com.googlecode.charts4j.BarChart;
import com.googlecode.charts4j.Data;
import com.googlecode.charts4j.DataUtil;
import com.googlecode.charts4j.GCharts;
import com.googlecode.charts4j.Plot;
import com.googlecode.charts4j.Plots;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Wrapper for the MapperStateEntity entity that holds state for
 * the controller tasks.
 *
 *
 */
public class MapperStateEntity<K, V, OK, OV> implements MapperState {
// --------------------------- STATIC FIELDS ---------------------------

  // Property names
  private static final String ACTIVE_SHARD_COUNT_PROPERTY = "activeShardCount";
  private static final String CHART_PROPERTY = "chart";
  private static final String COUNTERS_MAP_PROPERTY = "countersMap";
  private static final String LAST_POLL_TIME_PROPERTY = "lastPollTime";
  private static final String NAME_PROPERTY = "name";
  private static final String SHARD_COUNT_PROPERTY = "shardCount";
  private static final String START_TIME_PROPERTY = "startTime";
  private static final String STATUS_PROPERTY = "status";
  private static final String SPECIFICATION_PROPERTY = "specification";
  private static final String ENTITY_KIND = "MapperState";

// ------------------------------ FIELDS ------------------------------

  private Entity entity;

// --------------------------- CONSTRUCTORS ---------------------------

  /**
   * Initialize MapperStateEntity with the given datastore and a {@code null} entity.
   */
  private MapperStateEntity() {
    this(null);
  }

  /**
   * Initializes MapperStateEntity with the given datastore and entity.
   */
  private MapperStateEntity(Entity entity) {
    this.entity = entity;
  }

// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface MapperState ---------------------


  /**
   * Returns the current status: one of "active" or "done".
   *
   * @return the current status
   */
  @Override
  public Status getStatus() {
    return Status.valueOf((String) entity.getProperty(STATUS_PROPERTY));
  }

  /**
   * Reconstitutes a Counters object from a MR state entity.
   * The returned counters is a copy. You must call
   * {@link #setCounters(Counters)} to persist updated counters to the
   * datastore.
   *
   * @return the reconstituted Counters object
   */
  @Override
  public Counters getCounters() {
    return (Counters) SerializationUtil.deserializeFromByteArray(
        ((Blob) entity.getProperty(COUNTERS_MAP_PROPERTY)).getBytes());
  }

// --------------------- GETTER / SETTER METHODS ---------------------

  /**
   * Get the number of shards currently active.
   */
  public int getActiveShardCount() {
    return ((Long) entity.getProperty(ACTIVE_SHARD_COUNT_PROPERTY)).intValue();
  }

  /**
   * Set the number of active shard. Informative only.
   */
  public void setActiveShardCount(int activeShardCount) {
    entity.setProperty(ACTIVE_SHARD_COUNT_PROPERTY, activeShardCount);
  }

  /**
   * Get the Google Charts URL for this MR's status chart.
   */
  String getChartUrl() {
    return ((Text) entity.getProperty(CHART_PROPERTY)).getValue();
  }

  /**
   * Get the JobID for this MapperStateEntity.
   *
   * @return the JobID corresponding to this MapperStateEntity
   */
  public String getJobID() {
    return entity.getKey().getName();
  }

  /**
   * Returns the last time that we polled for quota updates.
   */
  public long getLastPollTime() {
    Long lastPollTime = (Long) entity.getProperty(LAST_POLL_TIME_PROPERTY);
    if (lastPollTime == null) {
      return -1;
    }
    return lastPollTime;
  }

  /**
   * Set the last poll time for future requests.
   *
   * @param time the time we last polled for quota updates in this request
   */
  public void setLastPollTime(long time) {
    entity.setProperty(LAST_POLL_TIME_PROPERTY, time);
  }

  /**
   * Get the human readable name for this MapReduce.
   */
  String getName() {
    return (String) entity.getProperty(NAME_PROPERTY);
  }

  /**
   * Set a human readable name for this MapReduce.
   */
  void setName(String name) {
    entity.setProperty(NAME_PROPERTY, name);
  }

  /**
   * Get the shard count. This is the total number of shards ever in existence
   * concurrently.
   */
  public int getShardCount() {
    return ((Long) entity.getProperty(SHARD_COUNT_PROPERTY)).intValue();
  }

  /**
   * Set the shard count. Informative only - the real number is set
   * as a property in the MR's configuration.
   */
  public void setShardCount(int shardCount) {
    entity.setProperty(SHARD_COUNT_PROPERTY, shardCount);
  }

  public MapperJobSpecification<K, V, OK, OV> getSpecification() {
    Blob serializedProperty = (Blob) entity.getProperty(SPECIFICATION_PROPERTY);
    return (MapperJobSpecification<K, V, OK, OV>) SerializationUtil
        .deserializeFromByteArray(serializedProperty.getBytes());
  }

  public void setSpecification(MapperJobSpecification<K, V, OK, OV> specification) {
    entity.setUnindexedProperty(SPECIFICATION_PROPERTY, new Blob(
        SerializationUtil.serializeToByteArray(specification)));
  }

  private byte[] getSpecificationBytes() {
    return ((Blob) entity.getProperty(SPECIFICATION_PROPERTY)).getBytes();
  }

  /**
   * Returns the time this MR was started.
   */
  long getStartTime() {
    return (Long) entity.getProperty(START_TIME_PROPERTY);
  }

  /**
   * Saves counters to the datastore entity.
   *
   * @param counters the counters to serialize
   */
  public void setCounters(Counters counters) {
    entity.setUnindexedProperty(COUNTERS_MAP_PROPERTY,
        new Blob(SerializationUtil.serializeToByteArray(counters)));
  }

  /**
   * Update this state to reflect the given set of mapper call counts.
   */
  public void setProcessedCounts(List<Long> processedCounts) {
    if (processedCounts == null || processedCounts.isEmpty()) {
      return;
    }

    // If max == 0, the numeric range will be from 0 to 0. This causes some
    // problems when scaling to the range, so add 1 to max, assuming that the
    // smallest value can be 0, and this ensures that the chart always shows,
    // at a minimum, a range from 0 to 1 - when all shards are just starting.
    long maxPlusOne = Collections.max(processedCounts) + 1;

    List<String> countLabels = new ArrayList<String>();
    for (int i = 0; i < processedCounts.size(); i++) {
      countLabels.add(String.valueOf(i));
    }

    Data countData = DataUtil.scaleWithinRange(0, maxPlusOne, processedCounts);

    // TODO(user): Rather than returning charts from both servers, let's just
    // do it on the client's end.
    Plot countPlot = Plots.newBarChartPlot(countData);
    BarChart countChart = GCharts.newBarChart(countPlot);
    countChart.addYAxisLabels(AxisLabelsFactory.newNumericRangeAxisLabels(0, maxPlusOne));
    countChart.addXAxisLabels(AxisLabelsFactory.newAxisLabels(countLabels));
    countChart.setSize(300, 200);
    countChart.setBarWidth(BarChart.AUTO_RESIZE);
    countChart.setSpaceBetweenGroupsOfBars(1);
    entity.setUnindexedProperty(CHART_PROPERTY, new Text(countChart.toURLString()));
  }

// -------------------------- INSTANCE METHODS --------------------------

  /**
   * Removes the underlying entity from the datastore. No other methods on
   * MapperStateEntity should be called after this one.
   */
  public void delete() {
    getDatastoreService().delete(entity.getKey());
  }

  /**
   * Save the MapperStateEntity to the datastore.
   */
  public void persist() {
    checkComplete();
    getDatastoreService().put(entity);
  }

  /**
   * Sets the status to "done"
   */
  public void setDone() {
    entity.setProperty(STATUS_PROPERTY, "" + Status.DONE);
  }

  /**
   * Create json object from this one. If detailed is true creates an object
   * with all the information needed for the job detail status view. Otherwise,
   * only includes the overview information.
   */
  public JSONObject toJson(boolean detailed) {
    JSONObject jobObject = new JSONObject();
    try {
      jobObject.put("name", getName());
      jobObject.put("mapreduce_id", getJobID());
      jobObject.put("active", getStatus() == Status.ACTIVE);
      jobObject.put("updated_timestamp_ms", getLastPollTime());
      jobObject.put("start_timestamp_ms", getStartTime());
      jobObject.put("result_status", String.valueOf(getStatus()));

      if (detailed) {
        jobObject.put("counters", toJson(getCounters()));
        jobObject.put("chart_url", getChartUrl());

        // TODO(user): Fill this from the Configuration
        JSONObject mapperSpec = new JSONObject();
        mapperSpec.put("mapper_params", new JSONObject());
        jobObject.put("mapper_spec", mapperSpec);

        List<ShardStateEntity<K, V, OK, OV>> shardStates = ShardStateEntity.getShardStates(
            this);

        JSONArray shardArray = new JSONArray();
        for (ShardStateEntity<K, V, OK, OV> shardState : shardStates) {
          shardArray.put(shardState.toJson());
        }
        jobObject.put("shards", shardArray);
      } else {
        jobObject.put("shards", getShardCount());
        jobObject.put("active_shards", getActiveShardCount());
      }
    } catch (JSONException e) {
      throw new RuntimeException("Hard coded string is null", e);
    }

    return jobObject;
  }

// -------------------------- OTHER METHODS --------------------------

  private void checkComplete() {
    Preconditions.checkNotNull(getSpecificationBytes(), "Specification must be set.");
  }

// -------------------------- STATIC METHODS --------------------------

  /**
   * Generates a MapperStateEntity that's configured with the given parameters, is
   * set as active, and has made no progress as of yet.
   *
   * The MapperStateEntity needs to have a configuration set via
   * {@code #setConfigurationXML(String)} before it can be persisted.
   *
   *
   * @param jobId the JobID this MapperStateEntity corresponds to
   * @param startTime start startTime for this MapReduce, in milliseconds from the epoch
   * @param name user visible name for this MapReduce
   * @return the initialized MapperStateEntity
   */
  public static <K, V, OK, OV> MapperStateEntity<K, V, OK, OV> createForNewJob(
      String name, String jobId, long startTime) {
    MapperStateEntity<K, V, OK, OV> state = new MapperStateEntity<K, V, OK, OV>();
    state.entity = new Entity(ENTITY_KIND, jobId);
    state.setName(name);
    state.entity.setProperty(STATUS_PROPERTY, "" + Status.ACTIVE);
    state.entity.setProperty(START_TIME_PROPERTY, startTime);
    state.entity.setUnindexedProperty(CHART_PROPERTY, new Text(""));
    state.setCounters(new CountersImpl());
    state.setActiveShardCount(0);
    state.setShardCount(0);
    return state;
  }

  /**
   * Gets the MapperStateEntity corresponding to the given job ID.
   *
   *
   * @param jobId the JobID to retrieve the MapperStateEntity for
   * @return the corresponding MapperStateEntity
   * @throws EntityNotFoundException if there is no MapperStateEntity corresponding
   * to the given JobID
   */
  public static <K, V, OK, OV> MapperStateEntity<K, V, OK, OV> getMapReduceStateFromJobID(String jobId) {
    Key key = KeyFactory.createKey(ENTITY_KIND, jobId);
    MapperStateEntity<K, V, OK, OV> state = new MapperStateEntity<K, V, OK, OV>();
    try {
      state.entity = getDatastoreService().get(key);
    } catch (EntityNotFoundException ignored) {
      return null;
    }
    return state;
  }

  /**
   * Gets a page of MapReduceStates.
   *
   * Given a cursor (possibly {@code null}) and a count, appends the page's
   * states to the {@code states} list, and returns a cursor for the next
   * page's position.
   */
  public static Cursor getMapReduceStates(
      String cursor, int count, Collection<MapperStateEntity<?, ?, ?, ?>> states) {
    FetchOptions fetchOptions = withPrefetchSize(count).limit(count);
    if (cursor != null) {
      fetchOptions = fetchOptions.startCursor(Cursor.fromWebSafeString(cursor));
    }
    QueryResultIterator<Entity> stateEntitiesIt = getDatastoreService().prepare(
        new Query(ENTITY_KIND)).asQueryResultIterator(fetchOptions);

    while (stateEntitiesIt.hasNext()) {
      states.add(new MapperStateEntity(stateEntitiesIt.next()));
    }
    return stateEntitiesIt.getCursor();
  }

  private static JSONObject toJson(Counters counters) throws JSONException {
    JSONObject retValue = new JSONObject();
    for (Counter counter : counters.getCounters()) {
      retValue.put(counter.getName(), counter.getValue());
    }

    return retValue;
  }
}
