package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;


/**
 * Compute the normalized entropy of a histogram.  The histogram is assumed to
 * be encoded as a map between values and counts (e.g. the output of
 * FB_HISTOGRAM).  The entropy is normalized in the sense that the counts are
 * first normalized so that the sum of all counts equals one (i.e., the
 * histogram is converted to a probability distribution).  If the histogram is
 * NULL then NULL is returned.  Any NULLs in the histogram itself are ignored.
 * If the histogram contains negative values then NULL is returned.  If the
 * total count of entries of in the histogram is zero (e.g. if the histogram
 * is empty), then zero is returned.
 *
 * Note that the entropy is computed using base-2 log.
 */
@Description(name = "udfmapentropy",
             value = "_FUNC_(histogram) - Return the normalized entropy of the histogram.")
public class UDFMapEntropy extends UDF {

  private static final double log2 = Math.log(2);

  public Double evaluate(HashMap<String, Double> histogram) {
    if (histogram == null) {
      return null;
    }

    double total = 0.0;
    for (Double value : histogram.values()) {
      if (value != null) {
        if (value >= 0) {
          total += value;
        } else {
          return null;
        }
      }
    }

    if (total == 0) {
      return Double.valueOf(0.0);
    }

    double entropy = 0.0;
    for (Double value : histogram.values()) {
      if (value != null && value > 0) {
        entropy -= (value / total) * Math.log(value / total);
      }
    }

    // Clip small negative values.
    if (entropy < 0) {
      entropy = 0;
    } else {
      entropy /= log2;
    }

    return Double.valueOf(entropy);
  }
}
