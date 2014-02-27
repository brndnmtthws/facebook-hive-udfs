package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

/**
 * Find the index where one array (the needle) first occurs as a
 * subsequence of the other (the haystack).  The return value is an
 * index into the haystack which starts at 0.  The function returns -1
 * if the needle is not found or is NULL.  If the haystack is NULL
 * then NULL is returned.  NULLs in haystack or needle are ignored.
 */
@Description(name = "udffindsequenceinarray",
             value = "_FUNC_(NEEDLE, HAYSTACK) - Find the index where one array (the needle) first occurs as a subsequence of the other (the haystack).")
public class UDFFindSequenceInArray extends UDF {
  public Integer evaluate(ArrayList<String> needle, ArrayList<String> haystack) {
    if (needle == null) {
      return Integer.valueOf(-1);
    }
    if (haystack == null) {
      return null;
    }
    for (int ii = 0; ii < haystack.size() - needle.size() + 1; ++ii) {
      boolean found = true;
      for (int jj = 0; jj < needle.size(); ++jj) {
        if (haystack.get(ii + jj) == null ||
            needle.get(jj) == null ||
            !haystack.get(ii + jj).equals(needle.get(jj))) {
            found = false;
            break;
        }
      }
      if (found) {
        return Integer.valueOf(ii);
      }
    }
    return Integer.valueOf(-1);
  }
}
