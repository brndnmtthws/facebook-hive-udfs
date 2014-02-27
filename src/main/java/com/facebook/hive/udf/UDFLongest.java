package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;


/**
 * Find the longest string among the arguments.  NULL arguments are ignored.
 * NULL is returned if there are no non-NULL arguments.  If there are multiple
 * strings of the longest length, then the first is returned.
 */
@Description(name = "udflongest",
             value = "_FUNC_(string...) - Return the longest string.")
public class UDFLongest extends UDF {
  public String evaluate(String... strs) {
    String longest = null;
    for (int ii = 0; ii < strs.length; ++ii) {
      if (strs[ii] == null) {
        continue;
      }
      if (longest == null || strs[ii].length() > longest.length()) {
        longest = strs[ii];
      }
    }
    if (longest == null) {
      return null;
    } else {
      return new String(longest);
    }
  }
}
