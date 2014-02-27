package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;


/**
 * Randomly sample an index with weight proportional to arguments.  If any of
 * the weights are NULL, negative, infinite, or not numerically interpretable
 * then NULL is returned.  The argument should be a series of doubles.  If N 
 * denotes the number of arguments, v[i] denotes the ith argument and V denotes
 * \sum_i v[i] then this function returns a sample from 0 to N-1 inclusive 
 * where j has probability v[j] / V of being chosen.
 */
@Description(name = "choose",
             value = "_FUNC_(v1, v2, ...) - Randomly samples an element " + 
                     "from a 0-indexed index with weight proportional to" +
                     "arguments.")
public class UDFChoose extends UDF {
  public Integer evaluate(Double... vals) {
    double sum = 0.0;
    for (int ii = 0; ii < vals.length; ++ii) {
      Double v = vals[ii];
      if (v == null || v < 0 || v.isNaN() || v.isInfinite()) {
        return null;
      }
      sum += vals[ii];
    }

    double r = Math.random() * sum ;
    for (int ii = 0; ii < vals.length; ++ii) {
      if (r < vals[ii]) {
        return Integer.valueOf(ii);
      }
      r -= vals[ii] ;
    }

    // In case of floating point precision issues, return index.
    return Integer.valueOf(vals.length - 1);
  }
}
