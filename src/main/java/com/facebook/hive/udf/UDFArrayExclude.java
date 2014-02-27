package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;



/**
 * Removes elements of the first array whose indices are present in the second
 * array.  The index of the first element is 0.  If either input is NULL then
 * NULL is returned.  Any NULLs in the index array are ignored. 
 */
@Description(name = "udfarrayexclude",
             value = "_FUNC_(values, indices) - Removes elements of 'values' whose indices are in 'indices'.")
public class UDFArrayExclude extends GenericUDF {
  ListObjectInspector arrayOI = null;
  ObjectInspectorConverters.Converter converter = null;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
    throws UDFArgumentException {

    ObjectInspector indexOI = 
      ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.javaIntObjectInspector);
      
    arrayOI = (ListObjectInspector)arguments[0];
    converter = 
      ObjectInspectorConverters.getConverter(arguments[1], indexOI);

    return arrayOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    List<?> value_array = arrayOI.getList(arguments[0].get());
    List<?> index_array = (List<?>)converter.convert(arguments[1].get());

    if (value_array == null || index_array == null) {
      return null;
    }

    // Avoid using addAll or the constructor here because that will cause
    // an unchecked cast warning on List<?>.
    HashSet<Integer> indices = new HashSet<Integer>();
    for (int ii = 0; ii < index_array.size(); ++ii) {
      indices.add((Integer)index_array.get(ii));
    }

    ArrayList<Object> result = new ArrayList<Object>();
    for (int ii = 0; ii < value_array.size(); ++ii) {
      if (!indices.contains(ii)) {
        result.add(value_array.get(ii));
      }
    }
  
    return result;
  }

  @Override
  public String getDisplayString(String[] input) {
	  return new String();
  }
}
