package com.facebook.hive.udf;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// package org.apache.hadoop.hive.contrib.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.LinkedList;

public final class UDAFTopN extends UDAF {
    public static class StringDoublePair implements Comparable<StringDoublePair> {
        public StringDoublePair() {
          this.key = null;
          this.value = null;
        }

	public StringDoublePair(String key, double value) {
	    this.key = key;
	    this.value = value;
	}

	public String getString() {
	    return this.key;
	}

	public int compareTo(StringDoublePair o) {
	    return o.value.compareTo(this.value);
	}

	private String key;
	private Double value;
    }

  //    public static class UDAFTopNException extends Exception {
  //      public static final long serialVersionUID = 1L;
  //    }

    /**
     * Note that this is only needed if the internal state cannot be represented
     * by a primitive.
     *
     * The internal state can also contains fields with types like
     * ArrayList<String> and HashMap<String,Double> if needed.
     */
    public static class UDAFTopNState {
	// The head of the queue should contain the smallest element.
      //	private PriorityQueue<StringDoublePair> queue;
      private LinkedList<StringDoublePair> queue;
      private Integer N;
    }

    /**
     * The actual class for doing the aggregation. Hive will automatically look
     * for all internal classes of the UDAF that implements UDAFEvaluator.
     */
    public static class UDAFTopNEvaluator implements UDAFEvaluator {

	UDAFTopNState state;

	public UDAFTopNEvaluator() {
	    super();
	    state = new UDAFTopNState();
	    init();
	}

	/**
	 * Reset the state of the aggregation.
	 */
	public void init() {
          //	    state.queue = new PriorityQueue<StringDoublePair>();
          state.queue = new LinkedList<StringDoublePair>();
	    state.N = null;
	}

	/**
	 * Iterate through one row of original data.
	 *
	 * The number and type of arguments need to the same as we call this UDAF
	 * from Hive command line.
	 *
	 * This function should always return true.
	 */
      //	public boolean iterate(String key, Double value, Integer N) throws UDAFTopNException {
	public boolean iterate(String key, Double value, Integer N) {
          //	    if (N == null || (state.N != null  && state.N != N)) {
          //		throw new UDAFTopNException();
          //	    }
	    if (state.N == null) {
		state.N = N;
	    }
	    if (value != null) {
		state.queue.add(new StringDoublePair(key, value));
		prune(state.queue, state.N);
	    }
	    return true;
	}

	/**
	 * Terminate a partial aggregation and return the state. If the state is a
	 * primitive, just return primitive Java classes like Integer or String.
	 */
	public UDAFTopNState terminatePartial() {
	    if (state.queue.size() > 0) {
		return state;
	    } else {
		return null;
	    }
	}

	/**
	 * Merge with a partial aggregation.
	 *
	 * This function should always have a single argument which has the same
	 * type as the return value of terminatePartial().
	 */
	public boolean merge(UDAFTopNState o) {
          //	public boolean merge(UDAFTopNState o) throws UDAFTopNException {
	    if (o != null) {
		state.queue.addAll(o.queue);
		if (o.N != state.N) {
                  //		    throw new UDAFTopNException();
		}
		prune(state.queue, state.N);
	    }
	    return true;
	}

      //	void prune(PriorityQueue<StringDoublePair> queue, int N) {
	void prune(LinkedList<StringDoublePair> queue, int N) {
	    while (queue.size() > N) {
              //		queue.remove();
              queue.removeLast();
	    }
	}

	/**
	 * Terminates the aggregation and return the final result.
	 */
	public LinkedList<String> terminate() {
	    LinkedList<String> result = new LinkedList<String>();
	    while (state.queue.size() > 0) {
              StringDoublePair p = state.queue.poll();
              result.addFirst(p.getString());
	    }
	    return result;
	}
    }

  //    private UDAFTopN() {
	// prevent instantiation
  //    }
}
