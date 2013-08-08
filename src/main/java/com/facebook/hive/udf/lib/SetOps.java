package com.facebook.hive.udf.lib;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


public class SetOps {

    /** exists only to save and be smart about computation **/
    public static class SetPair {
        // s1 is the smaller one.
        Set<String> s1;
        Set<String> s2;
        public SetPair(Collection<String> strings1, Collection<String> strings2) {
            if (strings1.size() < strings2.size()) {
                initSets(strings1,strings2);
            } else {
                initSets(strings2,strings1);
            }
        }
        public void initSets(Collection<String> strings1, Collection<String> strings2) {
            s1 = new HashSet<String>();
            s2 = new HashSet<String>();
            s1.addAll(strings1);
            s2.addAll(strings2);
        }
        public int intersectSize() {
            int count=0;
            for (String x : s1)
                if (s2.contains(x))
                    count += 1;
            return count;
        }
    }

    public static double jaccard(Collection<String> s1, Collection<String> s2) {
        SetPair p = new SetPair(s1,s2);
        if (s1.size()==0 && s2.size()==0) return 0;
        // don't calculate the union, that's more expense
//        double dice = 2.0 * p.intersectSize() / (s1.size() + s2.size());
        double AB = p.intersectSize();
        return AB / (s1.size() + s2.size() - AB);
    }

    public static double sampleCorrectedJaccard(Collection<String> sample1, Collection<String> sample2,
            int fullSize1, int fullSize2) {
        SetPair p = new SetPair(sample1,sample2);
        if (sample1.size()==0 && sample2.size()==0) return 0;
        double alpha = sample1.size()*1.0 / fullSize1;
        double beta = sample2.size()*1.0 / fullSize2;
        double correctedIntersectSize = p.intersectSize() *1.0 / alpha / beta;
        // sometimes goes to an impossible value
        int n = Math.min(fullSize1, fullSize2);
        correctedIntersectSize = Math.min(n, correctedIntersectSize);
        // |AB|/(|AvB|) = |AB|/(|A| + |B| - |AB|)
        return correctedIntersectSize / (fullSize1 + fullSize2 - correctedIntersectSize);
    }

    public static void main(String args[]) throws IOException {
        InputStreamReader converter = new InputStreamReader(System.in);
        BufferedReader in = new BufferedReader(converter);
        List<String> s1 = Arrays.asList( in.readLine().trim().split("\\s+") );
        List<String> s2 = Arrays.asList( in.readLine().trim().split("\\s+") );
        int big1 = Integer.parseInt(in.readLine().trim());
        int big2 = Integer.parseInt(in.readLine().trim());
        System.out.println(jaccard(s1,s2));
        System.out.println(sampleCorrectedJaccard(s1,s2, big1,big2));
    }
}
