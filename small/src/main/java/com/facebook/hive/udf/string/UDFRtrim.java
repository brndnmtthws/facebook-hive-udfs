/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.hive.udf.string;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;

import com.facebook.hive.udf.tests.HiveUnitTests;
import com.facebook.hive.udf.tests.HiveUnitTest;

/**
 * Returns the string s (first argument) with the characters
 * in the string chars (second argument) trimmed off the right
 * side.
 *
 * @author atfiore
 */
@Description(name = "udfrtrim",
             value = "_FUNC_(s, chars) - Return the string s " +
                     "with the characters in chars trimmed off " +
                     "the right.")
@HiveUnitTests(tests = {
  @HiveUnitTest(query = "SELECT FB_RTRIM(\"aaXaabbccddeeee\", \"abcde\") " +
                        "from dim_one_row;",
                result = "aaX"),
  @HiveUnitTest(query = "SELECT FB_RTRIM(\"aabbccddee\", \"xy\") " +
                        "from dim_one_row;",
                result = "aabbccddee"),
  @HiveUnitTest(query = "SELECT FB_RTRIM(\"\", \"abcde\") " +
                        "from dim_one_row;",
                result = ""),
  @HiveUnitTest(query = "SELECT FB_RTRIM(\"a\", \"a\") " +
                        "from dim_one_row;",
                result = ""),
  @HiveUnitTest(query = "SELECT FB_RTRIM(\"aabbccddee\", \"abcde\") " +
                        "from dim_one_row;",
                result = "")
})

public class UDFRtrim extends UDF {
  public String evaluate(String s, String chars) {
    if (s == null) {
      return null;
    }
    if (chars == null) {
      return s;
    }
    int i = 0;

    int j = s.length() - 1;
    boolean done = false;
    while (j >= i && !done) {
      if (chars.indexOf(s.charAt(j)) == -1) {
        done = true;
      } else {
        j--;
      }
    }

    if (j < i) {
      return new String();
    }

    // substring(begin, end) takes chars from s[begin] to s[end-1]
    return s.substring(i, j + 1);
  }

  public String evaluate(String s) {
    return this.evaluate(s, " \n\t");
  }
}
