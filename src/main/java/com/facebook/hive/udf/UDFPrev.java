package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;


/**
 * PREV returns the value of the argument from the previous row and
 * NULL for the first row.  For example,
 *
 * hive> SELECT sepal_width, FB_PREV(sepal_width) FROM jonchang_iris LIMIT 5;
 * 2.9     NULL
 * 3.1     2.9
 * 3.4     3.1
 * 3.5     3.4
 * 3.1     3.5
 *
 * Note that each instance of this UDF maintains its own state, i.e.,
 * PREV will not return previous rows if they span different mappers.
 * One application of PREV is "sessionization", that is, breaking up a
 * list of events into sessions such that the time difference between
 * any two events is less than some threshold.  E.g.,
 *
 * hive> SELECT user,
 *              IF(time - FB_PREV(time) > THRESHOLD OR
 *                 user <> FB_PREV(user) OR
 *                 FB_PREV(time) IS NULL, 1, 0)
 *              AS new_session_marker
 *       FROM (
 *         SELECT user, time
 *         FROM events
 *         DISTRIBUTE BY user
 *         SORT BY user, time
 *       ) A
 *
 * The UDF CUMSUM can be used on new_session_marker to get a unique id
 * for each session.
 */
@Description(name = "udfprev",
             value = "_FUNC_(x) - Returns the value of x on the previous" +
                     "row (and NULL on the first row).")
  public class UDFPrev extends GenericUDF {
    Object previous;
    ObjectInspector oi;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
      previous = null;
      oi = arguments[0];
      return ObjectInspectorUtils.getStandardObjectInspector(oi);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments)
      throws HiveException {
      Object retval = previous;
      previous = ObjectInspectorUtils.copyToStandardObject(arguments[0].get(),
                                                           oi);
      return retval;
    }

    @Override
    public String getDisplayString(String[] children) {
	  return new String();
    }
  }
