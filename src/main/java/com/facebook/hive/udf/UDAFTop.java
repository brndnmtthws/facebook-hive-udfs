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

/**
 * Compute (normalized) entropy over a series of values.
 *
 */
public final class UDAFTop extends UDAF {

    /**
     * Note that this is only needed if the internal state cannot be represented
     * by a primitive.
     *
     * The internal state can also contains fields with types like
     * ArrayList<String> and HashMap<String,Double> if needed.
     */
    public static class UDAFTopState {
	private double value;
	private String key;
    }

    /**
     * The actual class for doing the aggregation. Hive will automatically look
     * for all internal classes of the UDAF that implements UDAFEvaluator.
     */
    public static class UDAFTopEvaluator implements UDAFEvaluator {

	UDAFTopState state;

	public UDAFTopEvaluator() {
	    super();
	    state = new UDAFTopState();
	    init();
	}

	/**
	 * Reset the state of the aggregation.
	 */
	public void init() {
	    state.value = Double.NEGATIVE_INFINITY;
	    state.key = null;
	}

	/**
	 * Iterate through one row of original data.
	 *
	 * The number and type of arguments need to the same as we call this UDAF
	 * from Hive command line.
	 *
	 * This function should always return true.
	 */
	public boolean iterate(String key, Double value) {
	    if (value != null && value > state.value) {
		state.value = value;
		state.key = key;
	    }
	    return true;
	}

	/**
	 * Terminate a partial aggregation and return the state. If the state is a
	 * primitive, just return primitive Java classes like Integer or String.
	 */
	public UDAFTopState terminatePartial() {
	    return state;
	}

	/**
	 * Merge with a partial aggregation.
	 *
	 * This function should always have a single argument which has the same
	 * type as the return value of terminatePartial().
	 */
	public boolean merge(UDAFTopState o) {
	    if (o != null) {
		if (o.value > state.value) {
		    state.value = o.value;
		    state.key = o.key;
		}
	    }
	    return true;
	}

	/**
	 * Terminates the aggregation and return the final result.
	 */
	public String terminate() {
	    return state.key;
	}
    }

    private UDAFTop() {
	// prevent instantiation
    }
}
