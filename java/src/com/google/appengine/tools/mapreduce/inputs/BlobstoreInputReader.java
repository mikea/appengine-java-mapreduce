// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreInputStream;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.inputs.InputStreamIterator.OffsetRecordPair;
import com.google.common.io.CountingInputStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;

/**
 */
class BlobstoreInputReader extends InputReader<BlobstoreRecordKey, byte[]> {
// --------------------------- STATIC FIELDS ---------------------------

  private static final long serialVersionUID = -1869136825803030034L;
  private static final int DEFAULT_BUFFER_SIZE = 10000;

// ------------------------------ FIELDS ------------------------------

  long startOffset;
  long endOffset;
  private String blobKey;
  private byte terminator;
  private long offset = 0L;
  private transient CountingInputStream input;
  private transient Iterator<OffsetRecordPair> recordIterator;

// --------------------------- CONSTRUCTORS ---------------------------

  BlobstoreInputReader(String blobKey, long startOffset, long endOffset, byte terminator)
      throws IOException {
    this.blobKey = blobKey;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.terminator = terminator;

    createStreams();
  }

// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface Iterator ---------------------

  @Override
  public boolean hasNext() {
    return recordIterator.hasNext();
  }

  @Override
  public KeyValue<BlobstoreRecordKey, byte[]> next() {
    OffsetRecordPair next = recordIterator.next();

    BlobstoreRecordKey key = new BlobstoreRecordKey(
        new BlobKey(blobKey), startOffset + offset + next.getOffset());
    byte[] value = next.getRecord();

    return KeyValue.of(key, value);
  }

// ------------------------ IMPLEMENTING METHODS ------------------------

  @Override
  public double getProgress() {
    double currentOffset = (double) (offset + input.getCount());
    return currentOffset / (double) (endOffset - startOffset);
  }

// -------------------------- INSTANCE METHODS --------------------------

  private void createStreams() throws IOException {
    input = new CountingInputStream(
        new BufferedInputStream(
            new BlobstoreInputStream(new BlobKey(blobKey), startOffset + offset),
            DEFAULT_BUFFER_SIZE));

    recordIterator = new InputStreamIterator(input, endOffset - startOffset - offset,
        startOffset != 0L && offset == 0L,
        terminator);
  }

  private void readObject(ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    blobKey = (String) stream.readObject();
    startOffset = stream.readLong();
    endOffset = stream.readLong();
    terminator = stream.readByte();
    offset = stream.readLong();

    createStreams();
  }

  private void writeObject(ObjectOutputStream stream) throws IOException {
    stream.writeObject(blobKey);
    stream.writeLong(startOffset);
    stream.writeLong(endOffset);
    stream.writeByte(terminator);
    long newOffset = offset + (input == null ? 0 : input.getCount());
    stream.writeLong(newOffset);
  }
}
