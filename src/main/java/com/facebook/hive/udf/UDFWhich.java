package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

/**
 * Given an array of booleans, returns the indices of the TRUE values.
 */
@Description(name = "udfwhich",
             value = "_FUNC_(values) - Given an array of booleans, returns the (0-indexed) indices of the TRUE values.")

  public class UDFWhich extends UDF {
    public ArrayList<Integer> evaluate(Boolean... vals) {
      ArrayList<Integer> result = new ArrayList<Integer>();
      for (int ii = 0; ii < vals.length; ++ii) {
        if (vals[ii]) {
          result.add(ii);
        }
      }
      return result;
    }
  }
