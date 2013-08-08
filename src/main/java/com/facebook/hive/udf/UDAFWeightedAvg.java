package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * Compute the weighted average.  This replaces the common idiom:
 *
 *   SUM(value * weight) / SUM(weight)
 *
 * If either the value or the weight is NULL, then NULL is returned.  NULL is
 * also returned if there are no non-NULL rows are seen.
 */
public final class UDAFWeightedAvg extends UDAF {

  public static class UDAFWeightedAvgEvaluator implements UDAFEvaluator {

    public static final class State {
      public Double sum_value = null;
      public Double sum_weight = null;
    }
    
    State state = null;

    public UDAFWeightedAvgEvaluator() {
      super();
      state = new State();
      init();
    }

    public void init() {
      state.sum_value = null;
      state.sum_weight = null;
    }

    public boolean iterate(Double value, Double weight) {
      if (value == null || weight == null) {
        return true;
      }

      if (state.sum_value == null) {
        state.sum_value = value * weight;
      } else {
        state.sum_value += value * weight;
      }

      if (state.sum_weight == null) {
        state.sum_weight = weight.doubleValue();
      } else {
        state.sum_weight += weight;
      }
      return true;
    }

    public State terminatePartial() {
      return state;
    }

    public boolean merge(State other) {
      if (other == null) {
        return true;
      }

      if (other.sum_value != null) {
        if (state.sum_value == null) {
          state.sum_value = other.sum_value.doubleValue();
        } else {
          state.sum_value += other.sum_value;
        }
      }

      if (other.sum_weight != null) {
        if (state.sum_weight == null) {
          state.sum_weight = other.sum_weight.doubleValue();
        } else {
          state.sum_weight += other.sum_weight;
        }
      }
      return true;
    }

    public Double terminate() {
      if (state.sum_value == null || state.sum_weight == null) {
        return null;
      }
      return state.sum_value / state.sum_weight;
    }
  }
}
