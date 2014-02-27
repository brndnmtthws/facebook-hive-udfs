package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.ArrayList;


/**
 * Performs K-means on a set of points.  Each point is represented by an array
 * of DOUBLEs.  The set of points is passed in as the first argument as an
 * array of points (i.e., an array of arrays).  Each point should have the same
 * number of elements.  The second argument is an integer indicating the number
 * of clusters, K, to cluster the points into.  The last argument is an upper
 * bound on the number of iterations the procedure will execute (see below).
 * Note that since all the points are passed in as an array, this is a UDF and
 * not a UDAF.  If you wish to perform K-means where the points are distributed
 * across multiple rows, use FB_COLLECT to assemble the points into a single
 * row.
 *
 * The algorithm used here first selects initial cluster centers using the
 * K-means++ heuristic.  Then the standard Lloyd's algorithm is performed until
 * convergence or max_iterations iterations have elapsed.
 *
 * If any arguments are NULL then NULL is returned.
 *
 * The return value is an array of cluster centers.  Each center is an array of
 * DOUBLEs whose size is two greater than the dimensionality of the inputs.
 * Letting M denote the dimensionality of the inputs, then the first M elements
 * of each center contain the mean coordinates for that cluster.  The next
 * element contains the average squared distance of the points from the cluster
 * mean.  The last element contains the number of points assigned to that
 * cluster.  Note that the number of centers may be less than K if less then K
 * points were passed in in the first argument.
 */
@Description(name = "kmeans",
             value = "_FUNC_(points, K, max_iterations) - Perform K-means clustering on a collection of points represented as arrays and returns an array of cluster centers.")
public class UDFKmeans extends UDF {
  public int sample(double[] weights) throws SemanticException {
    double weight_sum = 0;
    for (int ii = 0; ii < weights.length; ++ii) {
      if (weights[ii] < 0.0) {
        return -3;
      }
      weight_sum += weights[ii];
    }

    double r = Math.random();

    if (weight_sum == 0.0) {
      return (int)(r * weights.length);
    }

    for (int ii = 0; ii < weights.length; ++ii) {
      if (r < weights[ii] / weight_sum) {
        return ii;
      }
      r -= weights[ii] / weight_sum;
    }

    return -2;
  }

  public double squared_dist(ArrayList<Double> center, ArrayList<Double> point) throws SemanticException {
    int M = point.size();
    if (M != center.size()) {
      throw new UDFArgumentTypeException(M,
          "This should never happen.");
    }

    double dist2 = 0;
    for (int mm = 0; mm < M; ++mm) {
      dist2 += (center.get(mm) - point.get(mm)) *
        (center.get(mm) - point.get(mm));
    }
    return dist2;
  }

  public ArrayList<ArrayList<Double>>
    evaluate(ArrayList<ArrayList<Double>> points,
        Integer K, Integer max_iterations) throws SemanticException {

      if (K == null || max_iterations == null || points == null) {
        return null;
      }

      if (K <= 0) {
        throw new UDFArgumentTypeException(K,
            "K should be positive.");
      }

      int N = points.size();
      // If we have fewer points than clusters, then just return the input.
      if (N < K) {
        for (int ii = 0; ii < N; ++ii) {
          points.get(ii).add(0.0);
          points.get(ii).add(1.0);
        }
        return points;
      }

      // First initialize using kmeans++.
      int M = points.get(0).size();
      ArrayList<ArrayList<Double>> centers = new ArrayList<ArrayList<Double>>();
      double dist2s[] = new double[N];

      for (int ii = 0; ii < K; ++ii) {
        int new_center = -1;

        if (ii > 0) {
          // Compute the distance to all centers.
          for (int jj = 0; jj < N; ++jj) {
            dist2s[jj] = 1e100;
            ArrayList<Double> point = points.get(jj);
            if (point.size() != M) {
              throw new UDFArgumentTypeException(M,
                  "Sizes of tuples do not match.");
            }
            for (int kk = 0; kk < ii; ++kk) {
              double dist2 = squared_dist(centers.get(kk), point);
              if (dist2 < dist2s[jj]) {
                dist2s[jj] = dist2;
              }
            }
          }
          // Select a new point.
          new_center = sample(dist2s);
        } else {
          new_center = (int)(Math.random() * N);
        }

        ArrayList<Double> point = points.get(new_center);
        if (point.size() != M) {
          throw new UDFArgumentTypeException(M,
              "Sizes of tuples do not match.");
        }
        // Note, we go through the following rigamarole to ensure we have a proper clone.
        ArrayList<Double> new_point = new ArrayList<Double>();
        for (int mm = 0; mm < M; ++mm) {
          new_point.add(point.get(mm).doubleValue());
        }
        centers.add(new_point);
      }

      int[] assignments = new int[N];
      int[] center_counts = new int[K];
      for (int jj = 0; jj < N; ++jj) {
        assignments[jj] = -1;
      }

      for (int ii = 0; ii < max_iterations; ++ii) {
        for (int kk = 0; kk < K; ++kk) {
          center_counts[kk] = 0;
        }
        boolean changed = false;

        // Compute the assignments.
        for (int jj = 0; jj < N; ++jj) {
          int old_assignment = assignments[jj];
          // Find the closest point.
          ArrayList<Double> point = points.get(jj);
          double mindist2 = 1e100;
          for (int kk = 0; kk < K; ++kk) {
            // Compute the distance to the kk'th center.
            double dist2 = squared_dist(centers.get(kk), point);
            if (dist2 < mindist2) {
              assignments[jj] = kk;
              mindist2 = dist2;
            }
          }
          if (assignments[jj] != old_assignment) {
            changed = true;
          }
          center_counts[assignments[jj]]++;
        }

        if (!changed) {
          break;
        }

        // Compute the means.
        for (int kk = 0; kk < K; ++kk) {
          for (int mm = 0; mm < M; ++mm) {
            centers.get(kk).set(mm, 0.0);
          }
        }
        for (int jj = 0; jj < N; ++jj) {
          ArrayList<Double> point = points.get(jj);
          ArrayList<Double> center = centers.get(assignments[jj]);
          for (int mm = 0; mm < M; ++mm) {
            center.set(mm, center.get(mm) +
                point.get(mm) / center_counts[assignments[jj]]);
          }
        }
      }

      // Compute sd and number of points in each cluster..
      ArrayList<ArrayList<Double>> extra_stats = new ArrayList<ArrayList<Double>>();
      for (int kk = 0; kk < K; ++kk) {
        ArrayList<Double> extra_stat = new ArrayList<Double>();
        extra_stat.add(0.0);
        extra_stat.add(0.0);
        extra_stats.add(extra_stat);
      }

      for (int jj = 0; jj < N; ++jj) {
        ArrayList<Double> point = points.get(jj);
        ArrayList<Double> center = centers.get(assignments[jj]);
        ArrayList<Double> extra_stat = extra_stats.get(assignments[jj]);
        extra_stat.set(1, extra_stat.get(1) + 1);
        extra_stat.set(0, extra_stat.get(0) + squared_dist(center, point));
      }

      for (int kk = 0; kk < K; ++kk) {
        double num_points = extra_stats.get(kk).get(1);
        centers.get(kk).add(extra_stats.get(kk).get(0) / num_points);
        centers.get(kk).add(num_points);
      }

      return centers;
    }
}
