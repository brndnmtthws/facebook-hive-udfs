package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;


/**
 * Convert a JSON-encoded array given as a string to a Hive array of strings.
 * Note that no further parsing is done of the elements of the array; if they
 * are JSON structures they will be returned as JSON strings.  NULL is returned
 * if either the input is NULL or could not be parsed.
 */
@Description(name = "udfjsonaarray",
             value = "_FUNC_(array_string) - Convert a string of a JSON-encoded array to a Hive array of strings.")
public class UDFJsonAsArray extends UDF {
  public ArrayList<String> evaluate(String jsonString) {
    if (jsonString == null) {
      return null;
    }
    try {
      JSONArray extractObject = new JSONArray(jsonString);
      ArrayList<String> result = new ArrayList<String>();
      for (int ii = 0; ii < extractObject.length(); ++ii) {
        result.add(extractObject.get(ii).toString());
      }
      return result;
    } catch (JSONException e) {
      return null;
    } catch (NumberFormatException e) {
      // Despite what the documentation says, JSONArray can also throw
      // NumberFormatExceptions for particular malformed strings.
      return null;
    }
  }
}
