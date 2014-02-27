package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.util.ArrayList;
import java.util.List;



/**
 * Concatenate the array arguments.  Arrays which are NULL are ignored.
 * Note that the return type is the same as the first array argument.
 * The types of subsequent arrays will be coerced to the type of
 * the first array.
 */
@Description(name = "udfarrayconcat",
             value = "_FUNC_(values) - Concatenates the array arguments")

public class UDFArrayConcat extends GenericUDF {
  ListObjectInspector arrayOI = null;
  ObjectInspectorConverters.Converter converters[];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
    throws UDFArgumentException {
    converters = new ObjectInspectorConverters.Converter[arguments.length];

    for (int ii = 0; ii < arguments.length; ++ii) {
      if (ii == 0) {
        arrayOI = (ListObjectInspector)ObjectInspectorUtils
            .getStandardObjectInspector(arguments[ii]);
      }
      converters[ii] = ObjectInspectorConverters.getConverter(arguments[ii], arrayOI);
    }
    return arrayOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    ArrayList<Object> result_array = null;
    for (int ii = 0; ii < arguments.length; ++ii) {
      List<?> array = (List<?>)converters[ii].convert(arguments[ii].get());
      if (array == null) {
        continue;
      }

      if (result_array == null) {
        result_array = new ArrayList<Object>(array);
      } else {
        result_array.addAll(array);
      }
    }
    return new ArrayList<Object>(result_array);
  }

    @Override
    public String getDisplayString(String[] children) {
	  return new String();
    }
}
