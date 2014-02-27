package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.*;


/**
 * Calculate weighted percentile values.  Here we use the definition given
 * below under the section 'Weighted percentile'.
 * http://en.wikipedia.org/wiki/Percentile
 *
 * Rows where either the weight or the value are NULL are discarded. The
 * percentile argument should be an array of values between 0 and 1; an
 * exception is thrown if this is not the case.
 *
 * Note that this is slightly different from a percentile computed by
 * replicating each 'value' 'weight' times, even if 'weight' is integral.  For
 * example, suppose we are given the following two rows: (0, 1), (1, 99).
 * Replicating and computing the median will yield 1 (under most median
 * algorithms) whereas the algorithm below will yield a number slightly less
 * than 1.
 */
@Description(name = "percentile",
    value = "_FUNC_(value, weight, pc) - Returns the weighted percentiles at" +
            " 'pc' of 'value' given 'weight'.")
public class UDAFWeightedPercentile extends UDAF {

  /**
   * A state class to store intermediate aggregation results.
   */
  public static class State {
    private Map<LongWritable, DoubleWritable> counts;
    private List<DoubleWritable> percentiles;
  }

  /**
   * A comparator to sort the entries in order.
   */
  public static class MyComparator
      implements Comparator<Map.Entry<LongWritable, DoubleWritable>> {
    @Override
    public int compare(Map.Entry<LongWritable, DoubleWritable> o1,
        Map.Entry<LongWritable, DoubleWritable> o2) {
      return o1.getKey().compareTo(o2.getKey());
    }
  }

  /**
   * Increment the State object with o as the key, and i as the count.
   */
  private static void increment(State s, LongWritable o, double i) {
    if (s.counts == null) {
      s.counts = new HashMap<LongWritable, DoubleWritable>();
    }
    DoubleWritable count = s.counts.get(o);
    if (count == null) {
      // We have to create a new object, because the object o belongs
      // to the code that creates it and may get its value changed.
      LongWritable key = new LongWritable();
      key.set(o.get());
      s.counts.put(key, new DoubleWritable(i));
    } else {
      count.set(count.get() + i);
    }
  }

  /**
   * Get the percentile value.  This follows the formula on Wikipedia under 
   * "Weighted percentile".
   * Using that notation,
   *   p_n = 100 / S_N (S_n - w_n / 2)
   *   v = v_k + (p - p_k) / (p_{k+1} - p_k) (v_{k+1} - v_{k})
   *
   * 'position' here is equivalent to S_N p / 100, denote this by P
   * Each entry in 'entriesList', e_n, is equal to S_n - w_n / 2
   */
  private static double getPercentile(
      List<Map.Entry<LongWritable, DoubleWritable>> entriesList,
      double position) {
    int k = 0;
    // while p_k < p
    //    => p_k S_N / 100 < p S_N / 100 = P
    //    => (S_k - w_k / 2) <  P
    //    => e_k < P
    while (k < entriesList.size() && 
           entriesList.get(k).getValue().get() < position) {
      k++;
    }
    if (k == entriesList.size()) {
      return entriesList.get(k - 1).getKey().get();
    }

    // p_k >= p
    double e_k = entriesList.get(k).getValue().get();
    long v_k = entriesList.get(k).getKey().get();
    if (e_k == position || k == 0) {
      return v_k;
    }

    // Need to interpolate since:
    // p_{k - 1} < p < p_k
    double e_km1 = entriesList.get(k - 1).getValue().get();
    long v_km1 = entriesList.get(k - 1).getKey().get();


    // p_{k+1} - p_k
    //   = 100 / S_N (e_{k+1} - e_k)
    // p - p_k
    //   = 100 / S_N (P - e_k)
    // (p - p_k) / (p_{k+1} - p_k) = (P - e_k) / (e_{k+1} - e_k)
    return v_km1 + (position - e_km1) / (e_k - e_km1) * (v_k - v_km1);
  }

  /**
   * The evaluator for percentile computation based on long for an array of
   * percentiles.
   */
  public static class PercentileLongArrayEvaluator implements UDAFEvaluator {

    private final State state;

    public PercentileLongArrayEvaluator() {
      state = new State();
    }

    public void init() {
      if (state.counts != null) {
        // We reuse the same hashmap to reduce new object allocation.
        // This means counts can be empty when there is no input data.
        state.counts.clear();
      }
    }

    public boolean iterate(LongWritable o, DoubleWritable w, 
                           List<DoubleWritable> percentiles) {
      if (state.percentiles == null) {
        for (int i = 0; i < percentiles.size(); i++) {
          if (percentiles.get(i).get() < 0.0 ||
              percentiles.get(i).get() > 1.0) {
            throw new RuntimeException("Percentile value must be in [0,1]");
          }
        }
        state.percentiles = new ArrayList<DoubleWritable>(percentiles);
      }
      if (o != null) {
        increment(state, o, 1);
      }
      return true;
    }

    public State terminatePartial() {
      return state;
    }

    public boolean merge(State other) {
      if (other == null || other.counts == null || other.percentiles == null) {
        return true;
      }

      if (state.percentiles == null) {
        state.percentiles = new ArrayList<DoubleWritable>(other.percentiles);
      }

      for (Map.Entry<LongWritable, DoubleWritable> e: other.counts.entrySet()) {
        increment(state, e.getKey(), e.getValue().get());
      }
      return true;
    }


    private List<DoubleWritable> results;

    public List<DoubleWritable> terminate() {
      // No input data
      if (state.counts == null || state.counts.size() == 0) {
        return null;
      }

      // Get all items into an array and sort them
      Set<Map.Entry<LongWritable, DoubleWritable>> entries = 
        state.counts.entrySet();
      List<Map.Entry<LongWritable, DoubleWritable>> entriesList =
        new ArrayList<Map.Entry<LongWritable, DoubleWritable>>(entries);
      Collections.sort(entriesList, new MyComparator());

      // accumulate the counts
      double total = 0.0;
      for (int i = 0; i < entriesList.size(); i++) {
        DoubleWritable count = entriesList.get(i).getValue();
        total += count.get();
        count.set(total - count.get() / 2);
      }

      // Initialize the results
      if (results == null) {
        results = new ArrayList<DoubleWritable>();
        for (int i = 0; i < state.percentiles.size(); i++) {
          results.add(new DoubleWritable());
        }
      }
      // Set the results
      for (int i = 0; i < state.percentiles.size(); i++) {
        double position = total * state.percentiles.get(i).get();
        results.get(i).set(getPercentile(entriesList, position));
      }
      return results;
    }
  }
}
