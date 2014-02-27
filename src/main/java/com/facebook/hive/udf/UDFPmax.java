package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Find the value of the largest argument.
 */
@Description(name = "udfpmax",
             value = "_FUNC_(...) - Find the value of the largest element.  Unlike MAX, PMAX finds the row-size maximum element (rather than the column-wise).  NULLs are ignored except when all arguments are NULL in which case NULL is returned.",
    extended = "Example:\n"
             + "  > SELECT BUCKET(foo, bar) FROM users;\n")
public class UDFPmax extends UDF {
    public Double evaluate(Double... args) {
	Double maxVal = null;
	for (int ii = 0; ii < args.length; ++ii) {
	    if (args[ii] != null) {
		if (maxVal == null || args[ii] > maxVal) {
		    maxVal = args[ii];
		}
	    }
	}
	return maxVal;
    }
}
