package com.facebook.hive.udf;

import com.facebook.hive.udf.lib.SetOps;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.List;

/**
 * Jaccard similarity |A&B| / |AvB|
 * of two sets, represented as arrays of strings (e.g. the output of COLLECT)
 */
@Description(name = "udfjaccard",
             value = "_FUNC_(array<string> set1, array<string> set2) ... or alternately \n" +
             "_FUNC_(array<string> set1sample, array<string> set2sample, int set1fullsize, int set2fullsize)\n",
    extended = "the sample-corrected version performs what we hope is an unbiased estimate. talk to @boconnor for gory details")
public class UDFJaccard extends UDF {
    public Double evaluate(List<String> set1, List<String> set2) {
        return SetOps.jaccard(set1, set2);
    }

    public Double evaluate(List<String> set1, List<String> set2, int fullSize1, int fullSize2) {
        return SetOps.sampleCorrectedJaccard(set1, set2, fullSize1, fullSize2);
    }
}
