package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * Returns true if ANY of the column of booleans passed to the function is true:
 *
 * If any item is "true", return true.
 * If there are no "true"s, but there are nulls, we don't know: Return null.
 * If all rows are false, return false.
 * If there are NO rows, there is no true item, return false.
 *
 * Semantically equivalent to:
 * NOT ALL(NOT column)
 */
public final class UDAFAny extends UDAF {

    public static class UDAFAnyEvaluator implements UDAFEvaluator {

        Boolean result = false;

        public UDAFAnyEvaluator() {
            super();
            init();
        }

        public void init() {
            result = false;
        }

        public boolean iterate(Boolean ThisBool) {
            if (result != null && result) {  // Finish state reached.
                return true;
            } else if (ThisBool == null) {  // We can no longer return false.
                result = null;
                return true;
            } else if (ThisBool) {  // Success!
                result = true;
                return true;
            } else {
                return true;
            }
        }

        public Boolean terminatePartial() {
          return result;
        }

        public boolean merge(Boolean soFar) {
            iterate(soFar);
            return true;
        }

        public Boolean terminate() {
            return result;
        }
    }
}
