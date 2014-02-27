package com.facebook.hive.udf;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;


/**
 * Title case a string.  Words are assumed to be delimited by whitespace.  NULL
 * is returned if the input is NULL.  The optional argument specifies whether
 * to "fully" capitalize the string (default true).  When this argument is
 * false the casing of words which are in all caps will not be modified.
 */
@Description(name = "title_case",
             value = "_FUNC_(string[, fully]) - Title case a string.")
public class UDFTitleCase extends UDF {
  public String evaluate(String s, Boolean fully) {
    if (s == null || fully == null) {
      return null;
    }
    if (fully) {
      return WordUtils.capitalizeFully(s);
    } else {
      return WordUtils.capitalize(s);
    }
  }
  
  public String evaluate(String s) {
    return evaluate(s, true);
  }
}
