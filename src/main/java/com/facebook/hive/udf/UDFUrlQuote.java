package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;


/**
 * UDF to URL quote a string.  This replaces special characters in the string
 * using '%xx' escapes.
 */
@Description(name = "udfurlquote",
             value = "_FUNC_(string) - URL quote 'string.'")
public class UDFUrlQuote extends UDF {
  public String evaluate(String s) throws UnsupportedEncodingException {
    if (s == null) {
      return null;
    }
    return URLEncoder.encode(s, "UTF-8");
  }
}
