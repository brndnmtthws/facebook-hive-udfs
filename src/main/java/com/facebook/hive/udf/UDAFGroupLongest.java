package com.facebook.hive.udf;

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

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public final class UDAFGroupLongest extends UDAF {
  public static class UDAFLongestState {
    private String longestString;
  }

  public static class UDAFLongestEvaluator implements UDAFEvaluator {

    UDAFLongestState state;

    public UDAFLongestEvaluator() {
      super();
      state = new UDAFLongestState();
      init();
    }

    public void init() {
      state.longestString = null;
    }

    public boolean iterate(String str) {
      if (str != null &&
          (state.longestString == null ||
           str.length() > state.longestString.length())) {
        state.longestString = new String(str);
      }
      return true;
    }

    public UDAFLongestState terminatePartial() {
      return state;
    }

    public boolean merge(UDAFLongestState o) {
      if (o != null && o.longestString != null) {
        if (state.longestString == null ||
            o.longestString.length() > state.longestString.length()) {
          state.longestString = o.longestString;
        }
      }
      return true;
    }

    public String terminate() {
      return state.longestString;
    }
  }

  private UDAFGroupLongest() {
    // prevent instantiation
  }
}
