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

/**
 * Consumer that prefetches quota from the given manager in batches.
 *
 * Call {@link #dispose()} to return any leftover prefetched quota
 * when you're done with the QuotaConsumer.
 *
 *
 */
public class QuotaConsumer {
// ------------------------------ FIELDS ------------------------------

  private final QuotaManager manager;
  private final String bucket;
  private final long batchSize;
  private long quota;

// --------------------------- CONSTRUCTORS ---------------------------

  /**
   *
   * @param manager the manager to consume quota from
   * @param bucket the name of the bucket from which to consume quota
   * @param batchSize the amount of quota to pull at a time from the manager
   */
  public QuotaConsumer(QuotaManager manager, String bucket, long batchSize) {
    this.manager = manager;
    this.bucket = bucket;
    this.batchSize = batchSize;
  }

// -------------------------- INSTANCE METHODS --------------------------

  /**
   * Check whether there is a sufficient available quota.
   * This doesn't reserve the quota, so this call may pass, but a subsequent
   * call to {@link #consume(long)} for the same amount may fail.
   *
   * @param amount amount of quota desired
   * @return true if there is sufficient quota available
   */
  public boolean check(long amount) {
    if (quota >= amount) {
      return true;
    }

    return quota + manager.get(bucket) >= amount;
  }

  /**
   * Consumes the given amount of quota.
   *
   * @param amount the amount of quota to consume
   * @return true if there was sufficient quota
   */
  public boolean consume(long amount) {
    while (quota < amount) {
      long delta = manager.consume(bucket, batchSize, true);
      if (delta == 0) {
        return false;
      }
      quota += delta;
    }

    quota -= amount;

    return true;
  }

  /**
   * Return any excess prefetched quota.
   */
  public void dispose() {
    manager.put(bucket, quota);
    quota = 0;
  }

  /**
   * Returns the given amount of quota.
   *
   * @param amount
   */
  public void put(long amount) {
    quota += amount;
  }
}
