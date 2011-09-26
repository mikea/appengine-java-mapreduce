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

package com.google.appengine.tools.mapreduce.impl.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 *
 */
public class SerializationUtil {
// --------------------------- CONSTRUCTORS ---------------------------

  private SerializationUtil() {
  }

// -------------------------- STATIC METHODS --------------------------

  public static Object deserializeFromByteArray(byte[] bytes) {
    ObjectInputStream in;
    try {
      in = new ObjectInputStream(new ByteArrayInputStream(bytes));
    } catch (IOException e) {
      throw new RuntimeException("Deserialization error", e);
    }
    try {
      return in.readObject();
    } catch (IOException e) {
      throw new RuntimeException("Deserialization error", e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Deserialization error", e);
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        throw new RuntimeException("Deserialization error", e);
      }
    }
  }

  public static byte[] serializeToByteArray(Serializable o) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(bytes);
      try {
        out.writeObject(o);
      } finally {
        out.close();
      }
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Can't serialize object: " + o, e);
    }
  }
}
