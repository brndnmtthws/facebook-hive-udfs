package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;



/**
 * Removes elements of the map (first argument) whose keys are present in the
 * array (second argument).  If either input is NULL then NULL is returned.
 * Any NULLs in the array are ignored. 
 */
@Description(name = "udfmapexclude",
             value = "_FUNC_(values, indices) - Removes elements of 'values'" +
                     " whose keys are in 'indices'.")
public class UDFMapExclude extends GenericUDF {
  ListObjectInspector arrayOI;
  MapObjectInspector mapOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {

    mapOI = (MapObjectInspector)arguments[0];
    arrayOI = (ListObjectInspector)arguments[1];

    // Check map against key.
    ObjectInspector mapItemOI = mapOI.getMapKeyObjectInspector();

    ObjectInspector listItemOI = arrayOI.getListElementObjectInspector();

    if (!ObjectInspectorUtils.compareTypes(mapItemOI, listItemOI)) {
      throw new UDFArgumentException("Map key type (" + mapItemOI + ") must match " + 
                                     "list element type (" + listItemOI + ").");
    }

    return ObjectInspectorUtils.getStandardObjectInspector(mapOI,
              ObjectInspectorCopyOption.WRITABLE);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Map<?,?> value_map = 
      (Map<?,?>)ObjectInspectorUtils.copyToStandardObject(arguments[0].get(), 
                                                          mapOI,
                                                          ObjectInspectorCopyOption.WRITABLE);
    List<?> index_array =
      (List<?>)ObjectInspectorUtils.copyToStandardObject(arguments[1].get(), 
                                                         arrayOI,
                                                         ObjectInspectorCopyOption.WRITABLE);

    if (value_map == null || index_array == null) {
      return null;
    }

    HashSet<Object> indices = new HashSet<Object>(index_array);

    HashMap<Object,Object> result = new HashMap<Object,Object>();
    for (Object key : value_map.keySet()) {
      if (!indices.contains(key)) {
        result.put(key, value_map.get(key));
      }
    }
  
    return result;
  }

  @Override
  public String getDisplayString(String[] input) {
	  return new String();
  }
}
