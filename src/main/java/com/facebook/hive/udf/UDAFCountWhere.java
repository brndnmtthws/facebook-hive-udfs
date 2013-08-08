package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * Compute a COUNT of row items in which a condition is true;
 * "true" means "not false and also not null."
 *
 * This sounds like it is the same as COUNT(1) with a WHERE or GROUP BY, but
 * it allows for multiple columns to be tracked separately within a single
 * aggregation.  This is faster and cleaner than
 * SUM(CAST(example = foo AS INT)), and also appropriately returns zero
 * when the item in question is NULL, unlike COUNT(1) which doesn't know what
 * is and isn't NULL.
 */
public final class UDAFCountWhere extends UDAF {

    /**
     * The actual class for doing the aggregation. Hive will automatically
     * look for all internal classes of the UDAF that implements
     * UDAFEvaluator.
     */
    public static class UDAFCountWhereEvaluator implements UDAFEvaluator {

        Integer c = 0;

	public UDAFCountWhereEvaluator() {
	    super();
	    init();
	}

	/**
	 * Reset the state of the aggregation.
	 */
	public void init() {
	    c = 0;
	}

	/**
	 * Iterate through one row of original data.
	 *
	 * The number and type of arguments need to the same as we call this UDAF
	 * from Hive command line.
	 *
	 * This function should always return true.
	 */
	public boolean iterate(Boolean ThisBool) {
	    if (ThisBool != null && ThisBool) {
	        c++;
            }
	    return true;
	}

	/**
	 * Terminate a partial aggregation and return the state. If the state is a
	 * primitive, just return primitive Java classes like Integer or String.
	 */
	public Integer terminatePartial() {
	    return c;
	}

	/**
	 * Merge with a partial aggregation.
	 *
	 * This function should always have a single argument which has the same
	 * type as the return value of terminatePartial().
	 */
	public boolean merge(Integer c1) {
	    c += c1;
	    return true;
	}

	/**
	 * Terminates the aggregation and return the final result.
	 */
	public Integer terminate() {
	    return c;
	}
    }
}
