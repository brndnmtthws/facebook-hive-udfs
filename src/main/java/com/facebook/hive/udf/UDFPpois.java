package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Compute the probability of an observation of a number of occurrances of
 * at least k given an expected rate of occurrance r. In other words, evaluate
 * the integral of the the Poisson distribution with lambda=r of points <= k.
 *
 * The first paramater, k, is the value you want to evaluate the likelihood
 * of; the second paramater, r, is the expected rate. PPOIS(k, r) is 
 * pronounced, "p pois of k given r."
 *
 * k is always an integer (or you may be misunderstanding this function). r
 * is potentially a double. For example, "If we usually get r=10.5 apples off of
 * this tree per week, and this week we got k=15, is that a lot?"
 * 1-FB_PPOIS(14, 10.5) says it is: Only 11% of the time will you get 15 or more.
 */
@Description(name = "ppois",
             value = "_FUNC_(k, r) - Evaluate the likelihood of observing k given expected rate r")
  public class UDFPpois extends UDF {

    public Double evaluate(Integer k, Double r) {
      if (k == null || r == null || k < 0 || r <= 0.0) {
        return null;
      }
      
      Double result = Math.exp(-r);  // result when k = 0
      if (k == 0) {
        return result;
      }
      
      Double logSum = 0.0;           // Math.log(1)
      Double logR = Math.log(r);
      for (int i = 1; i <= k; i++) {
        logSum = logSum + Math.log(i);
        result = result + Math.exp( i * logR - r - logSum );
      }
      return result;
    }
  }
