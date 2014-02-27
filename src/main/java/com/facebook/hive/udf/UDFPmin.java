package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Find the value of the smallest argument in a row.
 *
 * For an arbitrary number of inputs, they are up-converted to double.
 * Then, the smallest input is returned in double form. NULLs are
 * ignored, and NULL is returned if no non-NULL objects are passed.
 *
 */
@Description(name = "udfpmin",
             value = "_FUNC_(...) - Find the value of the smallest element.  Unlike MIN, PMIN finds the row-size minimum element (rather than the column-wise).  NULLs are ignored except when all arguments are NULL in which case NULL is returned.")
public class UDFPmin extends UDF {
    public Double evaluate(Double... args) {
	Double minVal = null;
	for (int ii = 0; ii < args.length; ++ii) {
	    if (args[ii] != null) {
		if (minVal == null || args[ii] < minVal) {
		    minVal = args[ii];
		}
	    }
	}
	return minVal;
    }
}
