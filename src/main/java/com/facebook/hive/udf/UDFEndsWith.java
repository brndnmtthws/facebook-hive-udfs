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
 * Returns true when the haystack string (first argument) ends with the
 * needle string (second argument).  If either argument is NULL then NULL is
 * returned.
 *
 * @author jonchang
 */
@Description(name = "ends_with",
             value = "_FUNC_(haystack, needle)")
public class UDFEndsWith extends UDF {
  public Boolean evaluate(String haystack, String needle) {
    if (haystack == null || needle == null) {
      return null;
    }
    return haystack.endsWith(needle);
  }
}
