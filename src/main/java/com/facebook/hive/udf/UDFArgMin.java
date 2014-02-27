package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Find the 0-indexed argument with the smallest value. NULLs are ignored.
 */
@Description(name = "udfargmin",
             value = "_FUNC_(double, double, ...) - Find the index with the smallest value")
  public class UDFArgMin extends UDF {
    public Integer evaluate(Double... args) {
      Integer which_min = null;
      Double min_val = Double.POSITIVE_INFINITY;
      for (int ii = 0; ii < args.length; ++ii) {
        if (args[ii] != null && args[ii] < min_val) {
          min_val = args[ii];
          which_min = ii;
        }
      }
      return which_min;
    }
  }
