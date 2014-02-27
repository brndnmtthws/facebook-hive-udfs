package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;


/**
 * For a given grouping, return one array from the grouping. Good when you
 * want to associate each key with an arbitrary value, or for when you are
 * certain that every item in the grouping is the same.
 *
 * This replaces UDAFFirst.
 *
 * Note that this function does not guarantee that you will get the first
 * item, only that you will get one of the items.
 *
 * TODO: Make this work with Hive STRUCT types.
 */
@Description(name = "chooseone", value="_FUNC_(value) - Return an arbitrary value from the group")
public final class UDAFChooseOne extends AbstractGenericUDAFResolver {
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentLengthException("Only one paramter expected, but you provided " + parameters.length);
    }
    return new Evaluator();
  }

  public static class Evaluator extends GenericUDAFEvaluator {
    ObjectInspector inputOI;
    
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      inputOI = parameters[0];
      return ObjectInspectorUtils.getStandardObjectInspector(inputOI);
    }
    
    static class State implements AggregationBuffer {
      Object state = null;
    }
    
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new State();
    }
    
    @Override
    public void iterate(AggregationBuffer agg, Object[] input) throws HiveException {
      State s = (State) agg;
      if (s.state == null) {
        s.state = ObjectInspectorUtils.copyToStandardObject(input[0], inputOI);
      }
    }
    
    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      State s = (State) agg;
      if (s.state == null) {
        s.state = ObjectInspectorUtils.copyToStandardObject(partial, inputOI);
      }
    }
    
    @Override
    public void reset(AggregationBuffer agg) {
      State s = (State) agg;
      s.state = null;
    }
    
    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      State s = (State) agg;
      return s.state;
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      State s = (State) agg;
      return s.state;
    }
  }
}
