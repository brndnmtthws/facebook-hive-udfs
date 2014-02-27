package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.util.Arrays;
import java.util.List;



/**
 * Sort the array passed as the parameter.
 */
@Description(name = "udfarraysort",
             value = "_FUNC_(values) - Sorts the given array.")
  public class UDFArraySort extends GenericUDF {
    ListObjectInspector arrayOI = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {

      arrayOI = (ListObjectInspector) arguments[0];
      return ObjectInspectorUtils.getStandardObjectInspector(arrayOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

      List<?> arr = (List<?>) ObjectInspectorUtils.copyToStandardObject(arguments[0].get(), arrayOI);
      Object[] arr_obj = arr.toArray();
      Arrays.sort(arr_obj);

      return Arrays.asList(arr_obj);
    }

    @Override
    public String getDisplayString(String[] input) {
	  return new String();
    }
  }
