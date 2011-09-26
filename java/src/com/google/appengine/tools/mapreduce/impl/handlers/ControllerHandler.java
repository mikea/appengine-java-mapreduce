// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.handlers;

import static com.google.appengine.tools.mapreduce.impl.handlers.WorkerHandler.getQuotaConsumerKey;

import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskAlreadyExistsException;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.mapreduce.Callback;
import com.google.appengine.tools.mapreduce.Counter;
import com.google.appengine.tools.mapreduce.CounterNames;
import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.Mapper;
import com.google.appengine.tools.mapreduce.MapperJobListener;
import com.google.appengine.tools.mapreduce.MapperJobSpecification;
import com.google.appengine.tools.mapreduce.Status;
import com.google.appengine.tools.mapreduce.impl.CountersImpl;
import com.google.appengine.tools.mapreduce.impl.MapperStateEntity;
import com.google.appengine.tools.mapreduce.impl.QuotaManager;
import com.google.appengine.tools.mapreduce.impl.ShardStateEntity;
import com.google.appengine.tools.mapreduce.impl.util.Clock;
import com.google.appengine.tools.mapreduce.impl.util.SystemClock;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

/**
 * Mapper controller logic handler.
 *
 */
public final class ControllerHandler<K, V, OK, OV> {
// --------------------------- STATIC FIELDS ---------------------------

  public static final int CONTROLLER_DELAY_MS = 2000;

// ------------------------------ FIELDS ------------------------------

  private final Clock clock;
  private final ControllerHandlerContext<K, V, OK, OV> context;

// --------------------------- CONSTRUCTORS ---------------------------

  private ControllerHandler(ControllerHandlerContext<K, V, OK, OV> context) {
    this(context, new SystemClock());
  }

  private ControllerHandler(ControllerHandlerContext<K, V, OK, OV> context, Clock clock) {
    this.context = context;
    this.clock = clock;
  }

// -------------------------- INSTANCE METHODS --------------------------

  /**
   * Update the current MR state by aggregating information from shard states.
   *
   * @param shardStates all shard states (active and inactive)
   */
  private void aggregateState(Iterable<ShardStateEntity<K, V, OK, OV>> shardStates) {
    List<Long> mapperCounts = new ArrayList<Long>();
    Counters counters = new CountersImpl();
    for (ShardStateEntity<K, V, OK, OV> shardState : shardStates) {
      Counters shardCounters = shardState.getCounters();
      mapperCounts.add(shardCounters.getCounter(CounterNames.MAPPER_CALLS).getValue());

      for (Counter shardCounter : shardCounters.getCounters()) {
        counters.getCounter(shardCounter.getName()).increment(shardCounter.getValue());
      }
    }

    MapperStateEntity<K, V, OK, OV> state = context.getState();
    state.setCounters(counters);
    state.setProcessedCounts(mapperCounts);
  }

  private void fillInitialQuotas(int numberOfShards) {
    Collection<Integer> shardNumbers = new ArrayList<Integer>();
    for (int i = 0; i < numberOfShards; ++i) {
      shardNumbers.add(i);
    }
    refillQuotas(shardNumbers);
  }

  private void handleControllerImpl() {
    MapperStateEntity<K, V, OK, OV> state = context.getState();
    if (state == null) {
      MapReduceServletImpl.LOG.warning("Mapper state not found, aborting: " + context.getJobId());
      return;
    }

    List<ShardStateEntity<K, V, OK, OV>> shardStates = ShardStateEntity.getShardStates(state);

    if (shardStates.size() != state.getShardCount()) {
      MapReduceServletImpl.LOG.warning("Missing shards for the job: " + context.getJobId());
      throw new IllegalStateException();
    }

    List<Integer> activeShardNumbers = ShardStateEntity.selectActiveShards(shardStates);

    aggregateState(shardStates);
    state.setActiveShardCount(activeShardNumbers.size());
    state.setShardCount(shardStates.size());

    if (activeShardNumbers.isEmpty()) {
      state.setDone();
    } else {
      refillQuotas(activeShardNumbers);
    }
    state.persist();

    if (state.getStatus() == Status.ACTIVE) {
      scheduleController();
    } else {
      ShardStateEntity.deleteAllShards(shardStates);
      MapperJobSpecification<K, V, OK, OV> specification = context.getSpecification();
      Mapper<K, V, OK, OV> mapper = specification.getMapper();
      if (mapper instanceof MapperJobListener) {
        //noinspection unchecked
        ((MapperJobListener<K, V, OK, OV>) mapper)
            .terminateJob(new MapperJobContextImpl<K, V, OK, OV>(context));
      }
      Callback doneCallback = specification.getDoneCallback();

      if (doneCallback != null) {
        doneCallback.schedule("done_callback:" + context.getJobId(), context.getJobId());
      }
    }
  }

  private String handleStartImpl() {
    MapperJobSpecification<K, V, OK, OV> specification = context.getSpecification();
    Input<K, V> input = specification.getInput();
    List<? extends InputReader<K, V>> sources = input.split(
        new MapperJobContextImpl<K, V, OK, OV>(context));

    MapperStateEntity<K, V, OK, OV> state = context.getState();

    // Abort if we don't have any splits
    if (sources == null || sources.isEmpty()) {
      state.setDone();
    }
    state.setActiveShardCount(sources.size());
    state.setShardCount(sources.size());
    state.persist();

    Mapper<K, V, OK, OV> mapper = specification.getMapper();
    if (mapper instanceof MapperJobListener) {
      //noinspection unchecked
      ((MapperJobListener<K, V, OK, OV>) mapper)
          .initializeJob(new MapperJobContextImpl<K, V, OK, OV>(context));
    }

    if (state.getStatus() == Status.ACTIVE) {
      fillInitialQuotas(sources.size());
      WorkerHandler.scheduleShardsForNewJob(context, sources);

      // It is important to start controller after we have created all shards. Otherwise we might
      // miss shard states.
      scheduleController();
    } else {
      if (mapper instanceof MapperJobListener) {
        //noinspection unchecked
        ((MapperJobListener<K, V, OK, OV>) mapper)
            .terminateJob(new MapperJobContextImpl<K, V, OK, OV>(context));
      }
    }

    return context.getJobId();
  }

  /**
   * Refills quotas for specific shard numbers based on the input processing rate.
   *
   * @param shardNumbers ids of shards to refill quota.
   */
  private void refillQuotas(Collection<Integer> shardNumbers) {
    if (shardNumbers.isEmpty()) {
      return;
    }
    MapperStateEntity<K, V, OK, OV> state = context.getState();

    long lastPollTimeMillis = state.getLastPollTime();
    long currentPollTimeMillis = clock.currentTimeMillis();

    int inputProcessingRate = context.getSpecification().getInputProcessingRate();
    long totalQuotaRefill;
    if (lastPollTimeMillis == -1L) {
    // Initial quota fill
      totalQuotaRefill = (long) inputProcessingRate;
    } else {
      long deltaMillis = currentPollTimeMillis - lastPollTimeMillis;
      //noinspection NumericCastThatLosesPrecision
      totalQuotaRefill = (long) (deltaMillis * inputProcessingRate / 1000.0);
    }
    long perShardQuotaRefill = totalQuotaRefill / shardNumbers.size();

    QuotaManager manager = new QuotaManager(MemcacheServiceFactory.getMemcacheService());
    for (int shardNumber : shardNumbers) {
      manager.put(getQuotaConsumerKey(context.getJobId(), shardNumber),
          perShardQuotaRefill);
    }
    state.setLastPollTime(currentPollTimeMillis);
  }

  /**
   * Schedules a controller task queue invocation.
   */
  private void scheduleController() {
    TaskOptions taskOptions = context.createTaskOptionsForNextSlice();

    try {
      QueueFactory.getQueue(context.getSpecification().getControllerQueueName()).add(taskOptions);
    } catch (TaskAlreadyExistsException e) {
      MapReduceServletImpl.LOG.warning("Controller task already exists: " + e.getMessage());
    }
  }

// -------------------------- STATIC METHODS --------------------------

  /**
   * Handle the initial request to start the MapReduce.
   *
   * @return the JobID of the newly created MapReduce or {@code null} if the MapReduce couldn't be
   *         created.
   */
  public static <K, V, OK, OV> String handleStart(
      MapperJobSpecification<K, V, OK, OV> specification, String baseUrl) {
    ControllerHandlerContext<K, V, OK, OV> context = ControllerHandlerContext.createForNewJob(
        generateNewJobId(), specification, baseUrl);

    return new ControllerHandler<K, V, OK, OV>(context).handleStartImpl();
  }

  private static String generateNewJobId() {
    return System.currentTimeMillis() + String.valueOf(UUID.randomUUID()).replace("-", "");
  }

  /**
   * Handle the abort_job AJAX command.
   */
  static JSONObject handleAbortJob(String jobId) {
    // TODO(user): Implement
    return new JSONObject();
  }

  /**
   * Handles the logic for a controller task queue invocation.
   *
   * The primary jobs of the controller are to aggregate state from the MR workers (e.g. presenting
   * an overall view of the counters), and to set the quota for MR workers.
   */
  static <K, V, OK, OV> void handleController(HttpServletRequest request) {
    ControllerHandlerContext<K, V, OK, OV> context = ControllerHandlerContext
        .createFromRequest(request);
    new ControllerHandler<K, V, OK, OV>(context).handleControllerImpl();
  }
}
