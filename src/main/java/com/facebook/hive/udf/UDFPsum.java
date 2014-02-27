package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Find the sum of elements in a list. If any are null, return null.
 */
@Description(name = "udfpsum",
    value = "_FUNC_(...) - Find the sum of arguments passed. Unlike SUM, PSUM adds up all of its arguments row-wise, rather than adding up a column of data. If any element is NULL, NULL is returned.")
public class UDFPsum extends UDF {
    public Double evaluate(Double... args) {
        Double psum = args.length > 0 ? 0.0 : null;
        for (int ii = 0; ii < args.length; ++ii) {
           if (args[ii] == null) {
             return null;
           }
           psum += args[ii];
        }
        return psum;
    }
}
