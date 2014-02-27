package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;



/**
 * Cast the type of the first argument to match the type of the second
 * argument.  This behaves like CAST except that this is implemented as a UDF
 * with standard UDF syntax, the second argument is not a typename but rather
 * an instance of a type, and it supports casting between non-primitive types
 * such as arrays.
 */
@Description(name = "udfcast",
             value = "_FUNC_(value1, value2) - Casts value1 so that it matches the type of value2.")
public class UDFCast extends GenericUDF {
  ObjectInspectorConverters.Converter converter;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {

    converter =
      ObjectInspectorConverters.getConverter(arguments[0], arguments[1]);

    return arguments[1];
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return converter.convert(arguments[0].get());
  }

  @Override
  public String getDisplayString(String[] input) {
	  return new String();
  }
}
