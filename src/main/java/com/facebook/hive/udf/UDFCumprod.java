package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Arrays;

@Description(name = "udfcumprod",
             value = "_FUNC_(VAL, KEYS...) - Computes a cumulative product on the VAL column.  Resets whenever KEYS... changes.")

  public class UDFCumprod extends UDF {
    Object previous_keys[] = null;
    Double running_prod;

    public Double evaluate(Double val, Object... keys) {
      if (previous_keys == null || !Arrays.equals(previous_keys, keys)) {
        running_prod = 1.0;
        previous_keys = keys.clone();
      }
      running_prod *= val;
      return running_prod;
    }
  }
