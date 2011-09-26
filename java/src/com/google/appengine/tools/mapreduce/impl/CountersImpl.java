// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.Counter;
import com.google.appengine.tools.mapreduce.Counters;

import java.io.Serializable;
import java.util.HashMap;

/**
 */
public class CountersImpl implements Counters {
// --------------------------- STATIC FIELDS ---------------------------

  private static final long serialVersionUID = -8499952345096458550L;

// ------------------------------ FIELDS ------------------------------

  private final HashMap<String, CounterImpl> values = new HashMap<String, CounterImpl>();

// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface Counters ---------------------

  @Override
  public Counter getCounter(String name) {
    CounterImpl counter = values.get(name);
    if (counter == null) {
      counter = new CounterImpl(name);
      values.put(name, counter);
    }
    return counter;
  }

  @Override
  public Iterable<? extends Counter> getCounters() {
    return values.values();
  }

// -------------------------- INNER CLASSES --------------------------

  private static class CounterImpl implements Counter, Serializable {
    private static final long serialVersionUID = 5872696485441192885L;

    private final String name;
    private long value = 0L;

    CounterImpl(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public long getValue() {
      return value;
    }

    @Override
    public void increment(long delta) {
      value += delta;
    }
  }
}
