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

package org.apache.flink.ml.examples.regression;

import org.apache.flink.ml.examples.ExampleOutputTestBase;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

/** Test for {@link LinearRegressionExample}. */
public class LinearRegressionExampleTest extends ExampleOutputTestBase {
    @Test
    public void testExample() {
        List<String> expectedOutput =
                Arrays.asList(
                        "Features: [2.0, 1.0] 	Expected Result: 4.0 	Prediction Result: 4.111804984334356",
                        "Features: [1.0, 2.0] 	Expected Result: 5.0 	Prediction Result: 4.800253632133046",
                        "Features: [4.0, 3.0] 	Expected Result: 10.0 	Prediction Result: 10.053177395312623",
                        "Features: [3.0, 2.0] 	Expected Result: 7.0 	Prediction Result: 7.0824911898234895",
                        "Features: [4.0, 3.0] 	Expected Result: 10.0 	Prediction Result: 10.053177395312623",
                        "Features: [5.0, 3.0] 	Expected Result: 11.0 	Prediction Result: 11.194296174157845",
                        "Features: [2.0, 2.0] 	Expected Result: 6.0 	Prediction Result: 5.941372410978268",
                        "Features: [2.0, 4.0] 	Expected Result: 10.0 	Prediction Result: 9.600507264266092");

        LinearRegressionExample.main(new String[0]);
        assertTrue(
                CollectionUtils.isEqualCollection(
                        expectedOutput, Arrays.asList(getOutputString().split("\n"))));
    }
}
