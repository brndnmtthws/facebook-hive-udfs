package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.Normalizer;


/**
 * Perform unicode normalization on a string.  See
 * http://unicode.org/reports/tr15/ for details on what unicode normalization
 * entails.  The following normalization forms are supported (passed in via the
 * second argument as a string):
 *   NFC
 *   NFD
 *   NFKC
 *   NFKD
 */
@Description(name = "udfnormalizeunicode",
             value = "_FUNC_(string, form) - Normalization the unicode 'string' to 'form.'")
public class UDFNormalizeUnicode extends UDF {
  public String evaluate(String s, String form) {
    if (s == null || form == null) {
      return null;
    }

    if (form.equals("NFC")) {
      return Normalizer.normalize(s, Normalizer.Form.NFC);
    } else if (form.equals("NFD")) {
      return Normalizer.normalize(s, Normalizer.Form.NFD);
    } else if (form.equals("NFKC")) {
      return Normalizer.normalize(s, Normalizer.Form.NFKC);
    } else if (form.equals("NFKD")) {
      return Normalizer.normalize(s, Normalizer.Form.NFKD);
    } else {
      return null;
    }
  }
}
