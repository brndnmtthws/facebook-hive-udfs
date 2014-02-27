/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.LinkedList;

public final class UDAFCollect extends AbstractGenericUDAFResolver {

  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
    throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
        "Exactly one argument is expected.");
    }

    return new GenericUDAFCollectEvaluator();
  }


  public static class GenericUDAFCollectEvaluator extends GenericUDAFEvaluator {
    ObjectInspector inputOI;
    ObjectInspector outputOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
      throws HiveException {
      assert(parameters.length == 1);
      super.init(m, parameters);
      inputOI = parameters[0];

      // This should be list of the inputOI.
      outputOI = ObjectInspectorFactory.getStandardListObjectInspector(
        ObjectInspectorUtils.getStandardObjectInspector(inputOI));
      return outputOI;
    }

    public static class UDAFCollectState implements AggregationBuffer {
      private LinkedList<Object> elements;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer()
      throws HiveException {
      UDAFCollectState myAgg = new UDAFCollectState();
      myAgg.elements = new LinkedList<Object>();
      return myAgg;
    }

    @Override
    public void reset(AggregationBuffer agg)
      throws HiveException {
      UDAFCollectState myAgg = (UDAFCollectState)agg;
      myAgg.elements.clear();
    }


    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
      throws HiveException {
      UDAFCollectState myAgg = (UDAFCollectState)agg;
      assert(parameters.length == 1);
      if (parameters[0] != null) {
        Object pCopy = ObjectInspectorUtils.copyToStandardObject(parameters[0],
                                                                 inputOI);
        myAgg.elements.add(pCopy);
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg)
      throws HiveException {
      UDAFCollectState myAgg = (UDAFCollectState)agg;
      if (myAgg.elements.size() == 0) {
        return null;
      } else {
        return myAgg;
      }
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
      throws HiveException {
      if (partial != null) {
        UDAFCollectState myAgg = (UDAFCollectState)agg;
        UDAFCollectState myPartial = (UDAFCollectState)partial;
        myAgg.elements.addAll(myPartial.elements);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg)
      throws HiveException {
      UDAFCollectState myAgg = (UDAFCollectState)agg;
      return myAgg.elements;
    }
  }
}
