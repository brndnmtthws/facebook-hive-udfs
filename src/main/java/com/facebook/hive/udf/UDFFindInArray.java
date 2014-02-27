package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

/**
 * Like 'FIELD', but operates on an Array.
 */
@Description(name = "udffindinarray",
             value = "_FUNC_(NEEDLE, HAYSTACK) - Find the first 1-indexed value of HAYSTACK which matches NEEDLE.  Returns NULL if HAYSTACK is NULL.  Returns 0 if NEEDLE is not found in HAYSTACK or is NULL.",
    extended = "Example:\n"
             + "  > SELECT FIND_IN_ARRAY(foo, bar) FROM users;\n")
  public class UDFFindInArray extends UDF {
      public Integer evaluate(String needle, ArrayList<String> haystack) {
	  if (needle == null) {
	      return Integer.valueOf(0);
	  }
	  if (haystack == null) {
	      return null;
	  }
	  int retval = 0;
	  for (int ii = 0; ii < haystack.size(); ++ii) {
            if (haystack.get(ii).equals(needle)) {
              retval = ii + 1;
            }
	  }
	  return Integer.valueOf(retval);
      }
  }
