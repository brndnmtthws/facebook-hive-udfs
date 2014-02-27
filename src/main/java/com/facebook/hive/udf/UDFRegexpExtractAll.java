package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.LinkedList;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Extract substrings of a string which satisfy a regular expression.  The
 * first argument is the haystack in which to search while the second is a
 * regular expression which follows Java's regular expression rules.  Note that
 * like REGEXP_EXTRACT, any backslashes in the regular expression string must
 * be escaped to account for Hive's backslash escaping (e.g., '\\w' instead of
 * '\w').
 *
 * An optional third argument specifies a group index.  If specified, the
 * function will only return the portion of the match in the nth group (the
 * expression enclosed by the nth opening parenthesis reading from the left).
 * Note that the special group value of 0 returns the entire matched string.
 *
 * The return value is an ARRAY of STRINGs of the extracted strings.  If any
 * argument is NULL then NULL is returned.
 *
 * Also note that the subsequences found will be non-overlapping; that is any
 * subsequence must start after the end of the previous subsequence. 
 */
@Description(name = "regexp_extract_all",
             value = "_FUNC_(haystack, pattern, [index]) - Find all the instances of pattern in haystack.")
public class UDFRegexpExtractAll extends UDF {
  private String lastRegex = null;
  private Pattern p = null;

  public LinkedList<String> evaluate(String s, String regex, 
                                     Integer extractIndex) {
    if (s == null || regex == null || extractIndex == null) {
      return null;
    }
    if (!regex.equals(lastRegex)) {
      lastRegex = regex;
      p = Pattern.compile(regex, Pattern.MULTILINE);
    }
    LinkedList<String> result = new LinkedList<String>();
    Matcher m = p.matcher(s);
    while (m.find()) {
      MatchResult mr = m.toMatchResult();
      result.add(mr.group(extractIndex));
    }
    return result;
  }

  public LinkedList<String> evaluate(String s, String regex) {
    return this.evaluate(s, regex, 0);
  }
}
