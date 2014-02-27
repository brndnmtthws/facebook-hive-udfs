package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONObject;

import java.util.Map;

/**
 * JSON-encode a Hive map.  If the input is NULL then NULL is returned.
 * NULLs within the input map are encoded as 'null'.  A reasonable attempt is
 * made to make sure the type of the values in the input map is preserved
 * in the JSON output; however some types such as STRUCTs may be converted to
 * strings before being encoded.  Additionally, the keys will always be
 * converted to strings.
 */
@Description(name = "make_json_obj",
             value = "_FUNC_(map) - JSON encode a Hive map.")
public class UDFMakeJSONObj extends GenericUDF {
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

    Map<?,?> map = (Map<?,?>)ObjectInspectorUtils.copyToStandardObject(
        arguments[0].get(),
        inputOI,
        ObjectInspectorCopyOption.JAVA);

    JSONObject json_object = new JSONObject(map);
    if (json_object == null) {
      return null;
    }
    return json_object.toString();
  }

  @Override
  public String getDisplayString(String[] input) {
	  return new String();
  }
}
