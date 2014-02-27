package com.facebook.hive.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;


/**
 * Convert a JSON-encoded map given as a string to a Hive map from strings to strings.
 * Note that no further parsing is done of the values of the map; if they are
 * JSON structures they will be returned as JSON strings.  NULL is returned
 * if either the input is NULL or could not be parsed.
 */
@Description(name = "udfjsonasmap",
             value = "_FUNC_(values) - Convert a JSON map to a Hive map.")
public class UDFJsonAsMap extends UDF {
  private static final Log LOG = LogFactory.getLog(UDFJsonAsMap.class.getName());
  private boolean warned = false;

  public HashMap<String, String> evaluate(String jsonString) throws JSONException {
    if (jsonString == null) {
      return null;
    }
    HashMap<String, String> result = null;
    try {
      JSONObject extractObject = new JSONObject(jsonString);
      result = new HashMap<String, String>();

      for (Iterator it = extractObject.keys(); it.hasNext(); ) {
        String key = (String)it.next();
        Object value = extractObject.get(key);
        result.put(key, value != null ? value.toString() : null);
      }
    } catch (JSONException e) {
      if (!warned) {
        LOG.warn("The JSON string '" + jsonString + "' can't be parsed", e);
        warned = true;
      }
      result = null;
    }
    return result;
  }
}
