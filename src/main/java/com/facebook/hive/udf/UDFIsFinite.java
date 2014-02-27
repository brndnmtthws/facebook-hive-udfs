package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Returns TRUE if this is a real number (not INF, not NAN), NULL if null.
 */
@Description(name = "udfisfinite",
             value = "_FUNC_(num) - Return TRUE if num is finite and a number.")
public class UDFIsFinite extends UDF {
    public Boolean evaluate(Double num) {
        if (num == null) {
          return null;
        }
        else {
          return !num.isNaN() && !num.isInfinite();
        }
    }
}
