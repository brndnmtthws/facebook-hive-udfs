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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;


/**
 * Returns the index where the needle string (first argument) occurs in the
 * haystack string (second argument).  The index begins at 0.  If the string
 * is not found then -1 is returned.  If either argument is NULL then NULL
 * is returned.
 *
 * @author jonchang
 */
@Description(name = "find_in_string",
             value = "_FUNC_(needle, haystack) - Return the index at which " +
                     "needle appears in haystack.")
public class UDFFindInString extends UDF {
  public Integer evaluate(String needle, String haystack) {
    if (haystack == null || needle == null) {
      return null;
    }
    return haystack.indexOf(needle);
  }
}
