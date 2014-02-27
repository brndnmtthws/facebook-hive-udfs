package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;


/**
 * GenericUDTFExplodeIndex: explode an array to multiple rows, along with the
 * index of the element in the array.
 */
@Description(name = "explode",
    value = "_FUNC_(a) - separates the elements of array a into multiple rows, "
        + "each row contains the element and the index within the array.  "
        + "The index starts from 0.")
public class UDTFExplodeIndex extends GenericUDTF {
    private ListObjectInspector listOI = null;
    private final Object[] forwardObj = new Object[] { null, new IntWritable() };

    @Override
    public void close() throws HiveException {
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] inputOIs) throws UDFArgumentException {

        listOI = (ListObjectInspector) inputOIs[0];

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("value");
        fieldNames.add("index");
        fieldOIs.add(listOI.getListElementObjectInspector());
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] o) throws HiveException {
        List<?> list = listOI.getList(o[0]);
        if (list == null) {
            return;
        }
        int index = 0;
        for (Object r : list) {
            forwardObj[0] = r;
            ((IntWritable) forwardObj[1]).set(index++);
            forward(forwardObj);
        }
    }
}
