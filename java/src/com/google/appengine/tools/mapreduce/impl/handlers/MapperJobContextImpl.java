// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.impl.handlers;

import com.google.appengine.tools.mapreduce.MapperJobContext;
import com.google.appengine.tools.mapreduce.MapperJobSpecification;

/**
 * Implementation of {@link MapperJobContext} that is given to the user code.
 *
 */
class MapperJobContextImpl<K, V, OK, OV> implements MapperJobContext<K, V, OK, OV> {
// ------------------------------ FIELDS ------------------------------

  private final HandlerContext<K, V, OK, OV> handlerContext;

// --------------------------- CONSTRUCTORS ---------------------------

  MapperJobContextImpl(HandlerContext<K, V, OK, OV> handlerContext) {
    this.handlerContext = handlerContext;
  }

// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface MapperJobContext ---------------------

  @Override
  public String getJobId() {
    return handlerContext.getJobId();
  }

  @Override
  public MapperJobSpecification<K, V, OK, OV> getSpecification() {
    return handlerContext.getSpecification();
  }
}
