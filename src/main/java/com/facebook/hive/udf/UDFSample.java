package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Description(name = "udfsample",
             value = "_FUNC_(N, A) - Randomly samples (at most) N elements from array A.")

  public class UDFSample extends GenericUDF {
    private ObjectInspectorConverters.Converter int_converter;
    private ListObjectInspector arrayOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
      if (arguments.length != 2) {
        throw new UDFArgumentLengthException("SAMPLE expects two arguments.");
      }

      if (!arguments[0].getCategory().equals(Category.PRIMITIVE)) {
        throw new UDFArgumentTypeException(0, "SAMPLE expects an INTEGER as its first argument");
      }

      int_converter = ObjectInspectorConverters.getConverter(arguments[0],
                                                             PrimitiveObjectInspectorFactory.writableIntObjectInspector);

      if (!arguments[1].getCategory().equals(Category.LIST)) {
        throw new UDFArgumentTypeException(1, "SAMPLE expects an ARRAY as its second argument");
      }

      arrayOI = (ListObjectInspector)arguments[1];
      return arguments[1];
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
      IntWritable intWritable = (IntWritable)int_converter.convert(arguments[0].get());
      if (intWritable == null) {
        return null;
      }

      int N = intWritable.get();
      if (N < 0) {
        throw new UDFArgumentException("SAMPLE requires a nonnegative number of elements to sample.");
      }

      List<?> array = arrayOI.getList(arguments[1].get());

      if (array == null) {
        return null;
      }

      if (N >= array.size()) {
        return arguments[1].get();
      }

      ArrayList<Object> array_copy = new ArrayList<Object>(array);

      Collections.shuffle(array_copy);
      return array_copy.subList(0, N);
    }

    @Override
    public String getDisplayString(String[] children) {
      assert (children.length == 2);
      StringBuilder sb = new StringBuilder();
      sb.append("fb_sample(")
        .append(children[0])
        .append(", ")
        .append(children[1])
        .append(")");
      return sb.toString();
    }
  }
