package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Find the 0-indexed argument with the largest value.
 */
@Description(name = "udfargmax",
             value = "_FUNC_(double, double, ...) - Find the index with the largest value",
    extended = "Example:\n"
             + "  > SELECT ARGMAX(foo, bar) FROM users;\n")
  public class UDFArgMax extends UDF {
    public Integer evaluate(Double... args) {
      Integer which_max = null;
      Double max_val = Double.NEGATIVE_INFINITY;
      for (int ii = 0; ii < args.length; ++ii) {
        if (args[ii] != null && args[ii] > max_val) {
          max_val = args[ii];
          which_max = ii;
        }
      }
      return which_max;
    }
  }
