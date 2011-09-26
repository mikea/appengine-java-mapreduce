// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.handlers;

import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskAlreadyExistsException;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.mapreduce.CounterNames;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.InputReader.KeyValue;
import com.google.appengine.tools.mapreduce.Mapper;
import com.google.appengine.tools.mapreduce.MapperContext;
import com.google.appengine.tools.mapreduce.MapperShardListener;
import com.google.appengine.tools.mapreduce.MapperSliceListener;
import com.google.appengine.tools.mapreduce.Status;
import com.google.appengine.tools.mapreduce.impl.QuotaConsumer;
import com.google.appengine.tools.mapreduce.impl.QuotaManager;
import com.google.appengine.tools.mapreduce.impl.ShardStateEntity;
import com.google.appengine.tools.mapreduce.impl.util.Clock;
import com.google.appengine.tools.mapreduce.impl.util.Stopwatch;
import com.google.appengine.tools.mapreduce.impl.util.SystemClock;

import javax.servlet.http.HttpServletRequest;

/**
 * Mapper Worker logic handler.
 *
 */
final class WorkerHandler<K, V, OK, OV> {
// --------------------------- STATIC FIELDS ---------------------------

  /**
   * Default amount of quota to divvy up per controller execution.
   */
  public static final long DEFAULT_QUOTA_BATCH_SIZE = 20;
  // Amount of time to spend on actual map() calls per task execution.
  private static final long PROCESSING_TIME_PER_TASK_MS = 10000;

// ------------------------------ FIELDS ------------------------------

  private final Clock clock;
  private final WorkerHandlerContext<K, V, OK, OV> context;

// --------------------------- CONSTRUCTORS ---------------------------

  public WorkerHandler(WorkerHandlerContext<K, V, OK, OV> context) {
    this(context, new SystemClock());
  }

  private WorkerHandler(WorkerHandlerContext<K, V, OK, OV> context, Clock clock) {
    this.context = context;
    this.clock = clock;
  }

// --------------------- GETTER / SETTER METHODS ---------------------

  /**
   * Get the QuotaConsumer for current shard.
   */
  private QuotaConsumer getQuotaConsumer() {
    QuotaManager manager = new QuotaManager(MemcacheServiceFactory.getMemcacheService());
    return new QuotaConsumer(
        manager,
        getQuotaConsumerKey(context.getJobId(), context.getShardNumber()),
        DEFAULT_QUOTA_BATCH_SIZE);
  }

// -------------------------- INSTANCE METHODS --------------------------

  void handleWorkerImpl() {
    ShardStateEntity<K, V, OK, OV> shardState = context.getShardState();

    if (shardState == null) {
      // Shard state has vanished. This is probably the task being executed
      // out of order by taskqueue.
      MapReduceServletImpl.LOG.warning("Shard state not found, aborting: " + context.getJobId());
      return;
    }

    if (shardState.getStatus() != Status.ACTIVE) {
      // Shard is not in an active state. This is probably the task being executed
      // out of order by taskqueue.
      MapReduceServletImpl.LOG.warning("Shard is not active, aborting: " + context.getJobId());
      return;
    }

    MapReduceServletImpl.LOG.fine(
        String.format("Running worker: %s %d %d",
            context.getJobId(),
            context.getShardNumber(),
            context.getSliceNumber()));

    Mapper<K, V, OK, OV> mapper = context.getSpecification().getMapper();

    MapperContextImpl<K, V, OK, OV> mapperContext = new MapperContextImpl<K, V, OK, OV>(context,
        context.getShardNumber());

    if (context.getSliceNumber() == 0 && mapper instanceof MapperShardListener) {
      // This is the first invocation for this mapper.
      //noinspection unchecked
      ((MapperShardListener<K, V, OK, OV>) mapper).initializeShard(mapperContext);
    }

    boolean shouldContinue = processMapper(mapper, mapperContext);

    if (!shouldContinue) {
      shardState.setStatus(Status.DONE);
    }

    mapperContext.flush();
    shardState.persist();

    if (shouldContinue) {
      scheduleWorker(context);
    } else {
      if (mapper instanceof MapperShardListener) {
        // This is the last invocation for this mapper.
        //noinspection unchecked
        ((MapperShardListener<K, V, OK, OV>) mapper).terminateShard(mapperContext);
      }
    }
  }

  private boolean processMapper(Mapper<K, V, OK, OV> mapper,
      MapperContext<K, V, OK, OV> mapperContext) {
    QuotaConsumer consumer = getQuotaConsumer();

    try {
      long startTime = clock.currentTimeMillis();
      boolean shouldShardContinue = true;
      if (consumer.check(1L)) {
        if (mapper instanceof MapperSliceListener) {
          //noinspection unchecked
          ((MapperSliceListener<K, V, OK, OV>) mapper).initializeSlice(mapperContext);
        }

        ShardStateEntity<K, V, OK, OV> shardState = context.getShardState();
        InputReader<K, V> inputReader = shardState.getInputReader();

        Stopwatch mapperStopwatch = new Stopwatch();

        int mapperCalls = 0;

        while (clock.currentTimeMillis() < startTime + PROCESSING_TIME_PER_TASK_MS) {
          if (!consumer.consume(1L)) {
            break;
          }
          if (!inputReader.hasNext()) {
            shouldShardContinue = false;
            break;
          }

          KeyValue<K, V> next = inputReader.next();

          try {
            mapperCalls++;
            mapperStopwatch.start();
            mapper.map(next.key, next.value, mapperContext);
          } finally {
            mapperStopwatch.stop();
          }
        }
        mapperContext.getCounters().getCounter(CounterNames.MAPPER_CALLS).increment(mapperCalls);
        mapperContext.getCounters().getCounter(CounterNames.MAPPER_WALLTIME_MSEC).increment(
            mapperStopwatch.getTimeMillis());

        if (mapper instanceof MapperSliceListener) {
          //noinspection unchecked
          ((MapperSliceListener<K, V, OK, OV>) mapper).terminateSlice(mapperContext);
        }
      } else {
        MapReduceServletImpl.LOG
            .info("Out of mapper quota. Aborting request until quota is replenished."
                + " Consider increasing processing rate if you would like your mapper job "
                + "to complete faster.");
      }

      return shouldShardContinue;
    } finally {
      consumer.dispose();
    }
  }

// -------------------------- STATIC METHODS --------------------------

  static String getQuotaConsumerKey(String jobId, int shardNumber) {
    return String.format("%s:%d", jobId, shardNumber);
  }

  static <K, V, OK, OV> void handleMapperWorker(HttpServletRequest request) {
    WorkerHandlerContext<K, V, OK, OV> context = WorkerHandlerContext
        .createFromRequest(request);
    new WorkerHandler<K, V, OK, OV>(context).handleWorkerImpl();
  }

  /**
   * Schedules the initial worker callback execution for all shards.
   */
  static <K, V, OK, OV> void scheduleShardsForNewJob(
      HandlerContext<K, V, OK, OV> context,
      Iterable<? extends InputReader<K, V>> sources) {
    int i = 0;
    for (InputReader<K, V> reader : sources) {
      ShardStateEntity<K, V, OK, OV> shardState = ShardStateEntity.createForNewJob(
          context.getJobId(), i, reader);
      shardState.persist();

      scheduleWorker(
          WorkerHandlerContext.createForNewJob(context.getJobId(), i, context.getBaseUrl()));
      i++;
    }
  }

  /**
   * Schedules a worker task on the appropriate queue.
   *
   * @param context the context for this MR job
   */
  private static <K, V, OK, OV> void scheduleWorker(WorkerHandlerContext<K, V, OK, OV> context) {
    TaskOptions taskOptions = context.createTaskOptionsForNextSlice();

    try {
      QueueFactory.getQueue(context.getSpecification().getWorkerQueueName()).add(taskOptions);
    } catch (TaskAlreadyExistsException ignored) {
      MapReduceServletImpl.LOG.warning("Worker task " + taskOptions + " already exists.");
    }
  }
}
