package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;


/**
 * ArrayJoin: Join an array using a separator. Just like "join" in every language ever.
 * This is very similar to CONCAT_WS, but operates on an array rather than a set of strings.
 *
 */

@Description(name = "udfarrayjoin",
             value = "_FUNC_(sep, arr) - Take every item in arr and concatenate it, putting the separator in between. Returns null if sep or arr is null; nulls in the array are ignored.",
    extended = "Example:\n"
             + "  > SELECT FB_ARRAY_JOIN(',', ARRAY(1, 2, 3)) FROM tmp_asmith_one_row;\n"
             + "Returns \"1,2,3\"\n")
public class UDFArrayJoin extends UDF {
    public String evaluate(String sep, ArrayList<String> arr) {
      if (sep == null || arr == null) {
        return null;
      } else if (arr.size() == 0) {
        return "";
      } else {
        String str = null;
        for (String item : arr) {
          if (item != null) {
            if (str != null) {
              str += sep;
            } else {
              str = "";
            }
            str += item;
          }
        }
        return str;
      }
    }
}
