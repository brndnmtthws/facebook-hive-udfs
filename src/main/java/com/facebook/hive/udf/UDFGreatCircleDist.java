package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Find the great-circle distance between two (lat,long) points.
 */
@Description(name = "udfgreatcircledist",
             value = "_FUNC_(double lat1, double long1, double lat2, double long2) - Find the great circle distance (in km) between two lat/long points (in degrees).",
	     extended = "")
  public class UDFGreatCircleDist extends UDF {
      public double evaluate(Double lat1, Double long1, Double lat2, Double long2) {

	  lat1 = lat1 * Math.PI / 180;
	  long1 = long1 * Math.PI / 180;
	  lat2 = lat2 * Math.PI / 180;
	  long2 = long2 * Math.PI / 180;

          double dlong = long1 - long2;

	  double t1 = Math.cos(lat2) * Math.sin(dlong);
	  double t2 = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dlong);
	  double t3 = Math.sin(lat1) * Math.sin(lat2) + Math.cos(lat1) * Math.cos(lat2) * Math.cos(dlong);
	  double ang_dist = Math.atan2(Math.sqrt(t1 * t1 + t2 * t2), t3);

	  return ang_dist * 6371.01;
    }
  }
