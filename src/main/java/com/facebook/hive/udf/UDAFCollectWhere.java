package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BooleanWritable;

import java.util.ArrayList;
import java.util.List;



/**
 * Aggregate the values which satisfy the condition into an array.  This
 * function is similar to COLLECT, except that it takes a second argument; only
 * rows which satisfy the condition in this second argument will be collected
 * and the rest will be ignored.  Rows with NULL values or NULL conditions will
 * be ignored.  Like COLLECT, you may need to turn off map-side aggregation
 * lest you exhaust the heap.
 */
@Description(name = "collect_where",
             value = "_FUNC_(value, condition) - aggregate the values which satisfy the condition into an array")
public class UDAFCollectWhere extends AbstractGenericUDAFResolver {
  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    return new Evaluator();
  }

  public static class State implements AggregationBuffer {
    ArrayList<Object> elements = new ArrayList<Object>();
  }

  public static class Evaluator extends GenericUDAFEvaluator {
    ObjectInspector inputOI;
    ListObjectInspector internalMergeOI;
    ObjectInspector conditionOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      if (m == Mode.COMPLETE || m == Mode.PARTIAL1) {
        inputOI = parameters[0];
        conditionOI = parameters[1];
        return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(inputOI));
      } else {
        internalMergeOI = (ListObjectInspector) parameters[0];
        return ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
      }
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new State();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] input) throws HiveException {
      if (input[0] != null && input[1] != null) {
        BooleanWritable condition = 
          (BooleanWritable)ObjectInspectorUtils.copyToStandardObject(input[1], conditionOI);
        if (condition.get()) {
          State state = (State) agg;
          state.elements.add(ObjectInspectorUtils.copyToStandardObject(input[0], inputOI));
        }
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        State state = (State) agg;
        state.elements.addAll((List<?>)ObjectInspectorUtils.copyToStandardObject(partial, internalMergeOI));
      }
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((State) agg).elements.clear();
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return ((State) agg).elements;
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return ((State) agg).elements;
    }
  }
}
