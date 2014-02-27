package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.List;

/**
 * JSON-encode a Hive array.  If the input is NULL then NULL is returned.
 * NULLs within the input array are encoded as 'null'.  A reasonable attempt is
 * made to make sure the type of the elements in the input array is preserved
 * in the JSON output; however some types such as STRUCTs may be converted to
 * strings before being encoded.
 */
@Description(name = "make_json_array",
             value = "_FUNC_(array) - JSON-encode a Hive array.")
public class UDFMakeJSONArray extends GenericUDF {
  ObjectInspector inputOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {

    inputOI = arguments[0];

    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }


  @Override
  public Object evaluate(DeferredObject[] arguments)
      throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }

    List<?> array = 
      (List<?>)ObjectInspectorUtils.copyToStandardObject(arguments[0].get(), 
                                                         inputOI,
                                                         ObjectInspectorCopyOption.JAVA);
    JSONArray json_array = new JSONArray(array);
    if (json_array == null) {
      return null;
    } 
    return json_array.toString();
  }

  @Override
  public String getDisplayString(String[] input) {
	  return new String();
  }
}
