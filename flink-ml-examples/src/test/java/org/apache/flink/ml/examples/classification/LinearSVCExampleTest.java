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

package org.apache.flink.ml.examples.classification;

import org.apache.flink.ml.examples.ExampleOutputTestBase;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

/** Test for {@link LinearSVCExample}. */
public class LinearSVCExampleTest extends ExampleOutputTestBase {
    @Test
    public void testExample() {
        List<String> expectedOutput =
                Arrays.asList(
                        "Features: [11.0, 2.0, 3.0, 4.0]     	Expected Result: 1.0 	Prediction Result: 1.0 	Raw Prediction Result: [1.2066666666666652, -1.2066666666666652]",
                        "Features: [3.0, 2.0, 3.0, 4.0]      	Expected Result: 0.0 	Prediction Result: 0.0 	Raw Prediction Result: [-2.553333333333334, 2.553333333333334]",
                        "Features: [2.0, 2.0, 3.0, 4.0]      	Expected Result: 0.0 	Prediction Result: 0.0 	Raw Prediction Result: [-3.023333333333334, 3.023333333333334]",
                        "Features: [5.0, 2.0, 3.0, 4.0]      	Expected Result: 0.0 	Prediction Result: 0.0 	Raw Prediction Result: [-1.6133333333333342, 1.6133333333333342]",
                        "Features: [1.0, 2.0, 3.0, 4.0]      	Expected Result: 0.0 	Prediction Result: 0.0 	Raw Prediction Result: [-3.493333333333334, 3.493333333333334]",
                        "Features: [15.0, 2.0, 3.0, 4.0]     	Expected Result: 1.0 	Prediction Result: 1.0 	Raw Prediction Result: [3.0866666666666647, -3.0866666666666647]",
                        "Features: [4.0, 2.0, 3.0, 4.0]      	Expected Result: 0.0 	Prediction Result: 0.0 	Raw Prediction Result: [-2.083333333333334, 2.083333333333334]",
                        "Features: [13.0, 2.0, 3.0, 4.0]     	Expected Result: 1.0 	Prediction Result: 1.0 	Raw Prediction Result: [2.146666666666665, -2.146666666666665]",
                        "Features: [14.0, 2.0, 3.0, 4.0]     	Expected Result: 1.0 	Prediction Result: 1.0 	Raw Prediction Result: [2.616666666666666, -2.616666666666666]",
                        "Features: [12.0, 2.0, 3.0, 4.0]     	Expected Result: 1.0 	Prediction Result: 1.0 	Raw Prediction Result: [1.676666666666665, -1.676666666666665]");

        LinearSVCExample.main(new String[0]);
        assertTrue(
                CollectionUtils.isEqualCollection(
                        expectedOutput, Arrays.asList(getOutputString().split("\n"))));
    }
}
