package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * Returns true if ALL of the column of booleans passed to the function are true.
 *
 * If any item is false, return false.
 * If there are no "false"s, but there are nulls, we don't know: Return null.
 * If all rows are true, return true.
 * If there are NO rows, both 0 are true and ALL are true. Return null.
 *
 * Semantically equivalent to:
 * NOT ANY(NOT column)
 */
public final class UDAFAll extends UDAF {

    public static class UDAFAllEvaluator implements UDAFEvaluator {

        Boolean result = null;
        Boolean any_rows_seen = false;

        public UDAFAllEvaluator() {
            super();
            init();
        }

        public void init() {
            result = null; // Return null for 0 rows
            any_rows_seen = false;
        }

        public boolean iterate(Boolean ThisBool) {
            if (result != null && !result) {  // Finish state reached.
              ;
            } else if (ThisBool == null) {  // We can no longer return true.
                result = null;
            } else if (!ThisBool) {  // Certainty! We have our answer.
                result = false;
            } else if (!any_rows_seen) {  // ThisBool must be true now
                result = true;  // Different initialization state for > 0 rows
            } else {
              ; // maintain current assumptions
            }
            any_rows_seen = true;
            return true;
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
