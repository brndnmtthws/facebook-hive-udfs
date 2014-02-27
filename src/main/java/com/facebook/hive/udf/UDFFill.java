package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "udffill",
             value = "_FUNC_(VAL) - Generates a column equal to VAL except when VAL is NULL in which case it uses the last non-null value.")

  public class UDFFill extends UDF {
    Double previous = null;

    public Double evaluate(Double val) {
      if (val == null) {
        return previous;
      } else {
        previous = val;
        return val;
      }
    }
  }
