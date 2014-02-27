package com.facebook.hive.udf;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;


/**
 * Unescape C-style escapes from a string.  For example, this replaces escapes
 * such as '\\n' and '\\t' with literal newlines and tabs.
 */
@Description(name = "udfunescape",
             value = "_FUNC_(string) - Unescape C-style escapes in 'string'.")
public class UDFUnescape extends UDF {
  public String evaluate(String s) {
    if (s == null) {
      return null;
    }
    return StringEscapeUtils.unescapeJava(s);
  }
}
