package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;


/**
 * Calculates Levenshtein distance between 2 strings.
 */
@Description(name = "udflevenshtein",
             value = "_FUNC_(string, string) - calculates Levenshtein distance between 2 strings.")
public class UDFLevenshtein extends UDF {
  public Integer evaluate(String s1, String s2) {
    if (s1 == null || s1 == null) {
      return null;
    }
    return computeLevenshteinDistance(s1.toCharArray(), 
      s2.toCharArray());
  }

  private static int minimum(int a, int b, int c) {
    if (a<=b && a<=c)
      return a;
    if (b<=a && b<=c)
	    return b;
    return c;
  }
    
  private static int computeLevenshteinDistance(char[] str1, char[] str2) {
    int[][] distance = new int[str1.length+1][];

    for(int i=0; i<=str1.length; i++) {
      distance[i] = new int[str2.length+1];
      distance[i][0] = i;
    }

    for(int j=0; j<str2.length+1; j++) {
      distance[0][j]=j;
    }

    for(int i=1; i<=str1.length; i++) {
      for(int j=1;j<=str2.length; j++) {
        distance[i][j]= minimum(distance[i-1][j]+1,
          distance[i][j-1]+1,
          distance[i-1][j-1]+((str1[i-1]==str2[j-1])?0:1));
      }
    }

    return distance[str1.length][str2.length];
  }
}
