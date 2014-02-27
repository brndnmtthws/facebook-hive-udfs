package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Converts a hex string to base-10 integer
 */
@Description(name = "udfhex2dec",
             value = "_FUNC_(string) - Convert hex (string form) to decimal",
    extended = "Example:\n"
             + "  > SELECT HEX2DEC('D') FROM hex;\n")
  public class UDFHex2Dec extends UDF {
    public Long evaluate(String hex_string) {
      try {
        return Long.valueOf(hex_string, 16).longValue();
      } catch(NumberFormatException e) {
        // This will catch null input values or invalid hex strings like 'g'
        return null;
      }
    }
  }
