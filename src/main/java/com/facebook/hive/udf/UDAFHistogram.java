package com.facebook.hive.udf;

import com.facebook.hive.udf.lib.Counter;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.Map;

/**
 * Compute counts over a discrete (String-valued) support.
 * Returns a map<string,int> data structure of the counts.
 * Seems to require:
 *
 *     set hive.map.aggr = false;
 *
 * Tests: see tests/Histogram.sql
 */
public class UDAFHistogram extends UDAF {

    /**
     * Note that this is only needed if the internal state cannot be represented
     * by a primitive.
     *
     * The internal state can also contains fields with types like
     * ArrayList<String> and HashMap<String,Double> if needed.
     */
    public static class UDAFHistogramState {
	private Counter<String> counter;
    }

    /**
     * The actual class for doing the aggregation. Hive will automatically look
     * for all internal classes of the UDAF that implements UDAFEvaluator.
     */
    public static class UDAFHistogramEvaluator implements UDAFEvaluator {

	private UDAFHistogramState state;

	public UDAFHistogramEvaluator() {
	    super();
	    state = new UDAFHistogramState();
	    init();
	}

	/**
	 * Reset the state of the aggregation.
	 */
	public void init() {
	    state.counter = new Counter<String>();
	}

	/**
	 * Iterate through one row of original data.
	 *
	 * The number and type of arguments need to the same as we call this UDAF
	 * from Hive command line.
	 *
	 * This function should always return true.
	 */
	public boolean iterate(String x) {
	    if (x != null) {
		state.counter.increment(x);
	    }
	    return true;
	}

	/**
	 * Terminate a partial aggregation and return the state. If the state is a
	 * primitive, just return primitive Java classes like Integer or String.
	 */
	public UDAFHistogramState terminatePartial() {
	    // Return null if we have no data.
	    if (state.counter.size() == 0) {
		return null;
	    } else {
		return state;
	    }
	}

	/**
	 * Merge with a partial aggregation.
	 *
	 * This function should always have a single argument which has the same
	 * type as the return value of terminatePartial().
	 */
	public boolean merge(UDAFHistogramState o) {
	    if (o != null) {
		state.counter.addAll(o.counter);
	    }
	    return true;
	}

	/**
	 * Terminates the aggregation and return the final result.
	 */
	public Map<String,Integer> terminate() {
	    return state.counter.counts;
	}
    }

    private UDAFHistogram() {
	// prevent instantiation
    }
}
