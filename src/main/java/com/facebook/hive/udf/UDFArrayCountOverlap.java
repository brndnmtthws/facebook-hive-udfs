package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * ArrayCountOverlap counts how many items in one array are also
 * in another array.
 *
 * Note that since NULL means "unknown," an unknown thing in one array does
 * not count as "found" in a second array just because arr2 has some other
 * unknown thing in it.
 *
 * If arr1 is empty, overlap is 0.
 */

@Description(name = "udfarraycountoverlap",
             value = "_FUNC_(array1, array2) - Counts how many items in array1 are also in array2")
  public class UDFArrayCountOverlap extends UDF {

    public Integer evaluate(ArrayList<String> arr1, ArrayList<String> arr2) {
      return arr1.size() > arr2.size() ? evaluate2(arr2, arr1) : evaluate2(arr1, arr2);
    }

    public Integer evaluate2(ArrayList<String> arr1, ArrayList<String> arr2) {
      // Be greedy, but not too greedy...limit map to 100M buckets but try for
      // 1000 buckets per potentially-used item. Should keep us under our
      // allotted default -Xmx512MB.
      Integer capacity = arr1.size() > 10485 ? 104857600 : arr1.size()*1000;
      HashMap<String, Integer> m = new HashMap<String, Integer>(capacity, (float) 1.0);
      
      Integer result = 0;
      for (String key : arr1) {
        if (key != null) {
          m.put(key, m.containsKey(key) ? m.get(key) + 1 : 1);
        }
      }

      Integer val;
      for (String key : arr2) {
        if (key != null) {
          if (m.containsKey(key) && (val = m.get(key)) > 0) {
            result++;
            m.put(key, val - 1);
          }
        }
      }
      return result;
    }

  }
