package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import java.util.List;



/**
 * Subsets an array given an offset (zero-indexed) and length.  
 * If offset is negative, the sequence will start that far from the end of the 
 * array.
 * If length is negative, the sequence will stop that many elements from the 
 * end of the array.
 * If length is not specified, the rest of the array is returned.
 *
 * If offset + length is greater than array length, the resulting array will 
 * be truncated.
 */
@Description(name = "udfarrayslice",
             value = "_FUNC_(values, offset, length) - Slices the given array as specified by the offset and length parameters.")
  public class UDFArraySlice extends GenericUDF {
    private ObjectInspectorConverters.Converter int_converter1;
    private ObjectInspectorConverters.Converter int_converter2;
    private ListObjectInspector arrayOI = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
      if (arguments.length != 2 && arguments.length != 3) {
        throw new UDFArgumentLengthException("Expected 2 or 3 inputs, got " +
            arguments.length);
      }
     
      int_converter1 = ObjectInspectorConverters.getConverter(arguments[1],
                                                             PrimitiveObjectInspectorFactory.writableIntObjectInspector);

      if (arguments.length == 3) {
        int_converter2 = ObjectInspectorConverters.getConverter(arguments[2],
                                                                PrimitiveObjectInspectorFactory.writableIntObjectInspector);
      }

      arrayOI = (ListObjectInspector) arguments[0];
      return ObjectInspectorUtils.getStandardObjectInspector(arrayOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
      if (arguments[0].get() == null) {
        return null;
      }   
      
      List<?> arr = (List<?>) ObjectInspectorUtils.copyToStandardObject(arguments[0].get(), arrayOI);

      IntWritable intWritable1 = (IntWritable)int_converter1.convert(arguments[1].get());
      if (intWritable1 == null) {
        return null;
      }
      int offset = intWritable1.get();
      if (offset < 0) {
        offset = arr.size() + offset;
      }

      int length, toIndex;

      if (arguments.length == 3) {
        IntWritable intWritable2 = (IntWritable)int_converter2.convert(arguments[2].get());
        if (intWritable2 == null) {
          return null;
        }
        length = intWritable2.get();
      
        if (length < 0) {
          toIndex = arr.size() + length;
        } else {
          toIndex = Math.min(offset + length, arr.size());
        }
      } else {
        // length not specified, return rest of array
        toIndex = arr.size();
      }

      if (offset >= toIndex || offset < 0 || toIndex < 0) {
        return null;
      }

      return arr.subList(offset, toIndex);
    }

    @Override
    public String getDisplayString(String[] input) {
	  return new String();
    }
  }
