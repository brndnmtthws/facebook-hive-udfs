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
import java.util.HashSet;
import java.util.List;



/**
 * Union the arrays passed as parameters.  This behaves like
 * ARRAY_CONCAT except that duplicates are removed (and order may not
 * be preserved).  Note that the return type is the same as the first
 * array argument.  The types of subsequent arrays will be coerced to
 * the type of the first array.
 */
@Description(name = "udfarrayunion",
             value = "_FUNC_(values) - Unions the array arguments, removing duplicates.")
  public class UDFArrayUnion extends GenericUDF {
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
      HashSet<Object> result_set = null;
      for (int ii = 0; ii < arguments.length; ++ii) {
        List<?> array = (List<?>)converters[ii].convert(arguments[ii].get());
        if (array == null) {
          continue;
        }

        if (result_set == null) {
          result_set = new HashSet<Object>(array);
        } else {
          result_set.addAll(array);
        }
      }
			if (result_set != null) {
				return new ArrayList<Object>(result_set);
			} else {
				return null;
			}
		}

		@Override
		public String getDisplayString(String[] input) {
			return new String();
		}
	}
