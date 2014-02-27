package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;


/**
 * Performs latent Dirichlet allocation (LDA) inference.  Note that this is
 * *NOT* linear discriminant analysis; this is a mixed-membership model of
 * discrete data.  For details on the model see Latent Dirichlet Allocation by
 * Blei et al. (2003).  The procedure assumes fixed topic multinomials; only
 * the parameters for a single document (per row) are
 * inferred.  
 *
 * Some of the parameters are stored as flattened matrices.  A flattened matrix
 * is a vector representation of a matrix.  If the matrix has N rows and M
 * columns, then the entry in position (i,j) of the original matrix is stored in
 * entry i + N * j of the flattened matrix.
 *
 * The arguments of the UDF are:
 *
 * words - an ARRAY of length W containing the words of the document.  Each
 *         word is represented by an INT 0-index into the columns of topics.
 *         That is, a value of i in 'words' represents the word indexed by the
 *         ith column of topics.
 * topics - a flattend matrix of dimension KxV, where K represents the number
 *          of topics and V the size of the vocabulary.  The value in the
 *          (i,j)th entry should be proportional to the probability of seeing
 *          word j in topic i.
 * initial - an initial guess for the document-topic distribution.  This should
 *           be an array of DOUBLEs of length K which sums to one.
 * alpha - Dirichlet hyperparameter for the prior on the document-topic
 *         distribution.
 * num_iterations - the number of iterations of inference to perform.
 *
 * Inference is performed using the CVB0 technique.
 *
 * If any of the arguments are NULL then NULL is returned.
 *
 * The return value is a vector of length K representing the inferred
 * topic loadings for that document.  It is an array of DOUBLEs whose
 * entries sum to one.
 */
@Description(name = "lda",
             value = "_FUNC_(words, topics, initial, alpha, num_iterations) -" + 
                     " Perform LDA inference on a document given by 0-indexed" + 
                     " words using the topics (which should be properly " + 
                     "normalized and smoothed.  Returns the topic proportions.")
public class UDFLDA extends UDF {
  public ArrayList<Double> evaluate(
      ArrayList<Integer> words, 
      ArrayList<Double> topics,
      ArrayList<Double> initial, 
      Double alpha,
      Integer num_iterations) {
  
    if (words == null || topics == null || initial == null || 
        alpha == null || num_iterations == null) {
        return null;
    }

    int K = initial.size();
    int Nw = words.size();

    if (K == 0 || num_iterations <= 0) {
      return null;
    }

    double[] document_sum = new double[K];
    double[] assignments = new double[K * Nw];

    // Initialize document_sum
    for (int kk = 0; kk < K; ++kk) {
      document_sum[kk] = initial.get(kk) * Nw;
      for (int ww = 0; ww < Nw; ++ww) {
        assignments[kk + ww * K] = initial.get(kk);
      }
    }

    for (int ii = 0; ii < num_iterations; ++ii) {
      for (int ww = 0; ww < Nw; ++ww) {
        int word = words.get(ww);
        double w_sum = 0.0;
        for (int kk = 0; kk < K; ++kk) {
          document_sum[kk] -= assignments[kk + ww * K];
        }
        for (int kk = 0; kk < K; ++kk) {
          assignments[kk + ww * K] = 
            (document_sum[kk] + alpha) * topics.get(kk + word * K);
          w_sum += assignments[kk + ww * K];
        }
        for (int kk = 0; kk < K; ++kk) {
          assignments[kk + ww * K] /= w_sum;
          document_sum[kk] += assignments[kk + ww * K];
        }
      }
    }

    // Normalize document_sum
    double sum = 0.0;
    for (int kk = 0; kk < K; ++kk) {
      sum += document_sum[kk];
    }
    ArrayList<Double> result = new ArrayList<Double>(K);
    for (int kk = 0; kk < K; ++kk) {
      if (sum == 0.0) {
        result.add(1.0 / K);
      } else {
        result.add(document_sum[kk] / sum);
      }
    }
    return result;
  }
}
