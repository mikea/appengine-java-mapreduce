// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.impl.handlers;

import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.MapperContext;
import com.google.appengine.tools.mapreduce.Pool;
import com.google.appengine.tools.mapreduce.Pool.PoolKey;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link MapperContext} that is given to the user code.
 *
 */
final class MapperContextImpl<K, V, OK, OV> extends MapperJobContextImpl<K, V, OK, OV>
    implements MapperContext<K, V, OK, OV> {
// ------------------------------ FIELDS ------------------------------

  private final WorkerHandlerContext<K, V, OK, OV> workerContext;
  private final int shardNumber;
  private final Map<PoolKey<?>, Pool> pools = new HashMap<PoolKey<?>, Pool>();

// --------------------------- CONSTRUCTORS ---------------------------

  MapperContextImpl(WorkerHandlerContext<K, V, OK, OV> workerContext, int shardNumber) {
    super(workerContext);
    this.workerContext = workerContext;
    this.shardNumber = shardNumber;
  }

// ------------------------ INTERFACE METHODS ------------------------

// --------------------- Interface MapperContext ---------------------

  @Override
  public Counters getCounters() {
    return workerContext.getShardState().getCounters();
  }

  @Override
  public MapperOutput<OK, OV> getOutput() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends Pool> T getPool(PoolKey<T> key) {
    @SuppressWarnings("unchecked") T pool = (T) pools.get(key);
    if (pool == null) {
      Class<? extends T> implClass = key.implClass;
      try {
        pool = implClass.getConstructor().newInstance();
      } catch (InstantiationException e) {
        throw new RuntimeException("Can't instantiate pool " + implClass, e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Can't instantiate pool " + implClass, e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException("Can't instantiate pool " + implClass, e);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException("Can't instantiate pool " + implClass, e);
      }
      pools.put(key, pool);
    }

    return pool;
  }

  @Override
  public int getShardNumber() {
    return shardNumber;
  }

// -------------------------- INSTANCE METHODS --------------------------

  public void flush() {
    for (Pool pool : pools.values()) {
      pool.flush();
    }
  }
}
