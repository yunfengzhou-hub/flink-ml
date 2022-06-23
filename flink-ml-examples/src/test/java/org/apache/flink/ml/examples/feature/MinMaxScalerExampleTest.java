/*
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

package org.apache.flink.ml.examples.feature;

import org.apache.flink.ml.examples.ExampleOutputTestBase;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

/** Test for {@link MinMaxScalerExample}. */
public class MinMaxScalerExampleTest extends ExampleOutputTestBase {
    @Test
    public void testExample() {
        List<String> expectedOutput =
                Arrays.asList(
                        "Input Value: [50.0, 40.0]   \tOutput Value: [0.25, 0.1]",
                        "Input Value: [150.0, 90.0]  \tOutput Value: [0.75, 0.225]",
                        "Input Value: [100.0, 50.0]  \tOutput Value: [0.5, 0.125]");

        MinMaxScalerExample.main(new String[0]);
        assertTrue(
                CollectionUtils.isEqualCollection(
                        expectedOutput, Arrays.asList(getOutputString().split("\n"))));
    }
}
