package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;



/**
 * Aggregate all the values of lists into an array. This is like collect_set(col),
 * which returns the same thing for columns of a primitive type. Because the
 * mappers must keep all of the data in memory, if your data is non-trivially
 * large you should set hive.map.aggr=false to ensure that UNION_SET is only
 *  executed in the reduce phase.
 * @author cbueno
 */
// The unitest result should properly be something like:
// ["3.4","3.5","3.6","3.7","3.8","4.4","3.9","4.2","4.1","3.1","3.2","3.3","2.2","2.3","2.4","2","4","3","2.9","2.5","2.6","2.7","2.8"]
// But since Sets can return elements in undefined order, I can't
// guarantee that what I get back will match the static string.
@Description(
  name = "union_set",
  value = "_FUNC_(col) - aggregate the values of an array column to one array",
  extended = "Aggregate the values, return as an ArrayList.")
public class UDAFUnionSet extends AbstractGenericUDAFResolver {
  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    return new Evaluator();
  }

  public static class State implements AggregationBuffer {
    HashSet<Object> set = new HashSet<>();
  }

  public static class Evaluator extends GenericUDAFEvaluator {
    ObjectInspector inputOI;
    ListObjectInspector internalMergeOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      if (m == Mode.COMPLETE || m == Mode.PARTIAL1) {
        inputOI = parameters[0];
      } else {
        internalMergeOI = (ListObjectInspector) parameters[0];
      }
      return ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new State();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] input) throws HiveException {
      if (input[0] != null) {
        State state = (State) agg;
        state.set.addAll((List<?>)ObjectInspectorUtils.copyToStandardObject(input[0], inputOI));
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        State state = (State) agg;
        List<?> pset = (List<?>)ObjectInspectorUtils.copyToStandardObject(partial, internalMergeOI);
        state.set.addAll(pset);
      }
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((State) agg).set.clear();
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return new ArrayList<>(((State) agg).set);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return new ArrayList<>(((State) agg).set);
    }
  }
}
