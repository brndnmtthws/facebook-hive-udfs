package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Arrays;

/**
 * This UDF computes the cumulative sum (initialized at 0).  Whenever
 * one of the key columns change, the sum is reinitialized.  Rows
 * where the value column is NULL do not contribute to the sum.
 *
 * Queries will typically first sort the data to ensure that the data
 * seen by this reducer is in the correct order.  For example, you
 * have a table of the number of user actions in each time bucket and
 * you want the cumulative number of actions for a user in each time
 * bucket,
 *
 * SELECT user, time, CUMSUM(num_actions, user) AS cumulative_actions
 * FROM (
 *   SELECT user, time, num_actions
 *   FROM your_table
 *   SORT BY user, time
 *   DISTRIBUTE BY user
 * ) A
 *
 * INPUT:
 * user time num_actions
 *    1    0           1
 *    2    0           2
 *    1    1           1
 *    2    1           1
 *
 * OUTPUT:
 * user time cumulative_actions
 *    1    0                  1
 *    1    1                  2
 *    2    0                  2
 *    2    1                  3
 */
@Description(name = "udfcumsum",
             value = "_FUNC_(VAL, KEYS...) - Computes a cumulative sum on the VAL column.  Resets whenever KEYS... changes.")

  public class UDFCumsum extends UDF {
    Object previous_keys[] = null;
    Double running_sum;

    public Double evaluate(Double val, Object... keys) {
      if (previous_keys == null || !Arrays.equals(previous_keys, keys)) {
        running_sum = 0.0;
        previous_keys = keys.clone();
      }
      if (val != null) {
        running_sum += val;
      }
      return running_sum;
    }
  }
