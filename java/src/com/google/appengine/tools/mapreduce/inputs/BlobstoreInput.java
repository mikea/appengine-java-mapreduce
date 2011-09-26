// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.blobstore.BlobInfoFactory;
import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.MapperJobContext;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class BlobstoreInput extends Input<BlobstoreRecordKey, byte[]> {
// --------------------------- STATIC FIELDS ---------------------------

  private static final long serialVersionUID = 2235444204028285444L;

// ------------------------------ FIELDS ------------------------------

  private final String blobKey;
  private final byte terminator;
  private final int shardCount;

// --------------------------- CONSTRUCTORS ---------------------------

  public BlobstoreInput(String blobKey) {
    this(blobKey, (byte) '\n');
  }

  public BlobstoreInput(String blobKey, byte terminator) {
    this(blobKey, terminator, 4);
  }

  public BlobstoreInput(String blobKey, byte terminator, int shardCount) {
    this.blobKey = blobKey;
    this.terminator = terminator;
    this.shardCount = shardCount;
  }

// ------------------------ IMPLEMENTING METHODS ------------------------

  @Override
  public List<? extends InputReader<BlobstoreRecordKey, byte[]>> split(
      MapperJobContext<BlobstoreRecordKey, byte[], ?, ?> context) {
    long blobSize = new BlobInfoFactory().loadBlobInfo(new BlobKey(blobKey)).getSize();
    return split(blobKey, blobSize, shardCount);
  }

// -------------------------- INSTANCE METHODS --------------------------

  private List<? extends InputReader<BlobstoreRecordKey, byte[]>> split(String blobKey,
      long blobSize, int shardCount) {
    try {
      Preconditions.checkNotNull(blobKey);
      Preconditions.checkArgument(shardCount > 0);
      Preconditions.checkArgument(blobSize >= 0);

      long splitLength = blobSize / shardCount;
      /*
        * Currently a single shard gets assigned a single split.
        * shardCount is only a hint for a number of splits we create.
        * If a shard workload is to small we want to reduce a number of shards.
        */
      if (splitLength == 0L) {
        splitLength = blobSize;
        shardCount = 1;
      }

      List<BlobstoreInputReader> result = new ArrayList<BlobstoreInputReader>();

      long startOffset = 0L;
      for (int i = 1; i < shardCount; i++) {
        long endOffset = (long) i * splitLength;
        result.add(new BlobstoreInputReader(blobKey, startOffset, endOffset, terminator));
        startOffset = endOffset;
      }
      result.add(new BlobstoreInputReader(blobKey, startOffset, blobSize, terminator));
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
