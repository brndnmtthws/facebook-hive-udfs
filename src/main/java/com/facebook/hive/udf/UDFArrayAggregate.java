package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;

import java.util.List;

  

/**
 * Aggregate the elements of the first argument (array) using the function
 * named in the second argument.  The function should be expressed as a string,
 * e.g., 'SUM' for the SUM function.  If the input array is NULL the NULL is
 * returned.  Otherwise, NULL behavior is as defined by the referenced
 * aggregation function.  The second argument should be constant and never
 * NULL.
 * 
 * Because of limitations in Hive, the return type of the UDAF cannot be
 * determined in advance.  Therefore the return type will be the same as the
 * element type of the input array.  An attempt is made to coerce the output of
 * the UDAF to the element type.  Additionally, the referenced function must be
 * unary.
 */
@Description(name = "array_aggregate",
             value = "_FUNC_(array, function) - Aggregate elements of the array"+
                                              " using the function in the UDAF.")
public class UDFArrayAggregate extends GenericUDF {
  private ListObjectInspector listOI = null;
  private ObjectInspector stringOI = null;
  private ObjectInspector elementOI = null;
  private GenericUDAFEvaluator evaluator = null;
  private ObjectInspectorConverters.Converter converter = null;
  private AggregationBuffer buffer = null;
  private Object[] inputArray = new Object[1];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) 
    throws UDFArgumentException {

    listOI = (ListObjectInspector)arguments[0];
    stringOI = arguments[1];
    elementOI = ObjectInspectorUtils.getStandardObjectInspector(
        listOI.getListElementObjectInspector());

    // Extra special magic.  Any function which performs reflection should call
    // may have happaned in a different thread than the one in which we are
    // currently executing.  Hence the static function registry may be 
    // initialized to its default value.

    return elementOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    List<?> inputs = (List<?>)
      ObjectInspectorUtils.copyToStandardObject(arguments[0].get(), listOI);

    if (inputs == null) {
      return null;
    }

    if (evaluator == null) {
      Text functionName = 
        (Text)ObjectInspectorUtils.copyToStandardObject(arguments[1].get(), stringOI);
  
      if (functionName == null) {
        throw new HiveException("Function name cannot be null.");
      }

      GenericUDAFResolver resolver =
        FunctionRegistry.getGenericUDAFResolver(functionName.toString());
      if (resolver == null) {
        throw new HiveException("Could not find function with name " + 
                                functionName.toString());
      }
      
      ObjectInspector[] objectInspectorArray = new ObjectInspector[1];
      objectInspectorArray[0] = elementOI;
     
      TypeInfo[] typeInfoArray = new TypeInfo[1];
      typeInfoArray[0] = 
        TypeInfoUtils.getTypeInfoFromObjectInspector(elementOI);
      
      evaluator = resolver.getEvaluator(typeInfoArray);
      converter = ObjectInspectorConverters.getConverter(
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, objectInspectorArray),
        elementOI);
      buffer = evaluator.getNewAggregationBuffer();
    } 

    evaluator.reset(buffer);

    for (Object input : inputs) {
      inputArray[0] = input;
      evaluator.iterate(buffer, inputArray);
    }

    return converter.convert(evaluator.terminate(buffer));
  }

  @Override
  public String getDisplayString(String[] input) {
	  return new String();
  }
}
