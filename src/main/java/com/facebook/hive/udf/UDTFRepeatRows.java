package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;

/**
 * Repeats each row an arbitrary number of times.  If the parameter is
 * null, then the row will not be repeated.
 *
 * Example usage:
 * hive> SELECT * FROM jonchang_iris LATERAL VIEW REPEAT_ROWS(IF(petal_width > 2.0, 3, 2)) a AS b LIMIT 7;
 * 6 2.9 4.5 1.5 versicolor 2
 * 6 2.9 4.5 1.5 versicolor 2
 * 6.9 3.1 5.1 2.3 virginica 3
 * 6.9 3.1 5.1 2.3 virginica 3
 * 6.9 3.1 5.1 2.3 virginica 3
 * 5.4 3.4 1.5 0.4 setosa 2
 * 5.4 3.4 1.5 0.4 setosa 2
 */
@Description(name = "repeat_rows",
             value = "_FUNC_(x) - outputs x copies of each row")
public class UDTFRepeatRows extends GenericUDTF {

  private PrimitiveObjectInspector colOI = null;

  @Override
  public void close() throws HiveException {
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length != 1) {
      throw new UDFArgumentException("repeat_rows() requires exactly one argument");
    }

    if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentException("repeat_rows() expects an integer argument");
    }

    colOI = (PrimitiveObjectInspector)args[0];
    if (colOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
      throw new UDFArgumentException("repeat_rows() expects an integer argument");
    }

    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    fieldNames.add("col0");
    fieldOIs.add(colOI);
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                                                                   fieldOIs);
  }

  @Override
  public void process(Object[] o) throws HiveException {
    Integer val = (Integer) colOI.getPrimitiveJavaObject(o[0]);
    if (val == null) {
      return;
    }

    if (val < 0) {
      throw new HiveException("repeat_rows() expects a non-negative argument");
    }

    for (int ii = 0; ii < val; ++ii) {
      forward(o);
    }
  }

  @Override
  public String toString() {
    return "repeat_rows";
  }
}
