package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;

/**
 * Find the bucket the first argument belongs to.  The buckets are
 * defined by the subsequent arguments.  Anything falling below the
 * first bucket is marked as 0, and anything above the last bucket is
 * marked as N, where N denotes the number of bucket parameters (i.e.,
 * total parameters - 1).  Ties are broken by choosing the lowest
 * bucket.
 */
@Description(name = "udfbucket",
             value = "_FUNC_(double, ...) - Find the bucket the first argument belongs to",
    extended = "Example:\n"
             + "  > SELECT BUCKET(foo, 0, 1, 2) FROM users;\n")
public class UDFBucket extends UDF {
    public Integer evaluate(Double value, Double... buckets) {
	if (value == null) {
	    return null;
	}
	for (int ii = 0; ii < buckets.length; ++ii) {
	    if (value <= buckets[ii]) {
              return Integer.valueOf(ii);
	    }
	}
	return Integer.valueOf(buckets.length);
    }

    public Integer evaluate(Double value, ArrayList<Double> buckets) {
	if (value == null) {
	    return null;
	}
	for (int ii = 0; ii < buckets.size(); ++ii) {
          if (value <= buckets.get(ii)) {
              return Integer.valueOf(ii);
	    }
	}
	return Integer.valueOf(buckets.size());
    }
}
