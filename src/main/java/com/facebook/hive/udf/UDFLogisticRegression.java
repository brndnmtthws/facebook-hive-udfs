package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "udflogisticregression",
             value = "_FUNC_(values) - Randomly samples from an 0-indexed index with weight proportional to arguments.")

  public class UDFLogisticRegression extends UDF {
    public Integer evaluate(Double... vals) {
      double sum = 0.0;
      for (int ii = 0; ii < vals.length; ++ii) {
        if (vals[ii] == null) {
          return null;
        }
        sum += vals[ii];
      }

      double r = Math.random();
      for (int ii = 0; ii < vals.length; ++ii) {
        if (r < vals[ii] / sum) {
          return Integer.valueOf(ii);
        }
        r -= vals[ii] / sum;
      }

      assert false;
      return null;
    }
  }
