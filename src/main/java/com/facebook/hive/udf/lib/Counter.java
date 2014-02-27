package com.facebook.hive.udf.lib;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** inspired by stanford nlp's Counter classes
 * @boconnor
 **/
public class Counter<E> {
    public Map<E,Integer> counts;
    public int totalCount;

    public Counter() {
        counts = new HashMap<E,Integer>();
        totalCount = 0;
    }
    public void increment(E obj, int amount) {
        if (!counts.containsKey(obj)) {
            counts.put(obj, amount);
        } else {
            counts.put(obj,  counts.get(obj) + amount);
        }
        totalCount += amount;
    }
    public void increment(E obj) {
        increment(obj, 1);
    }
    public int getCount(E obj) {
        if (!counts.containsKey(obj)) {
            return 0;
        } else {
            return counts.get(obj);
        }
    }
    public int size() {
        return counts.size();
    }
    public void addAll(Counter<E> counter) {
        for (E key : counter.keySet()) {
            increment(key, counter.getCount(key));
        }
    }
    public Set<E> keySet() {
        return counts.keySet();
    }

    /** this version is probabilities not counts, and also decorated with totalCount **/
    public String toNormalizedJSON() throws JSONException {
        JSONObject j = new JSONObject();
        j.put("totalCount", totalCount);
        JSONObject probs = new JSONObject();
        j.put("probs", probs);
        // If keys aren't Strings, problems could happen.
        for (E key : keySet()) {
            probs.put(key.toString(), getCount(key)*1.0 / totalCount);
        }
        return j.toString();
    }

    /** output should be something like:
     *
     * % echo a a a a a a b | tr ' ' '\n' | java -cp fb_udf.jar:$HIVE_HOME/lib/json.jar Counter
     * {"b":1,"a":6}
     * {"probs":{"b":0.14285714285714285,"a":0.8571428571428571},"totalCount":7}
     */
    public static void main(String args[]) throws Exception {
        InputStreamReader converter = new InputStreamReader(System.in);
        BufferedReader in = new BufferedReader(converter);
        String line = "";
        Counter<String> c = new Counter<String>();
        while ((line = in.readLine()) != null) {
            line = line.trim();
            c.increment(line);
        }

        System.out.println(new JSONObject(c.counts).toString());
        System.out.println(c.toNormalizedJSON());

    }
}
