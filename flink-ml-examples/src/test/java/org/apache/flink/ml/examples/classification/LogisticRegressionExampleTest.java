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

/** Test for {@link LogisticRegressionExample}. */
public class LogisticRegressionExampleTest extends ExampleOutputTestBase {
    @Test
    public void testExample() {
        List<String> expectedOutput =
                Arrays.asList(
                        "Features: [14.0, 2.0, 3.0, 4.0]     	Expected Result: 1.0 	Prediction Result: 1.0 	Raw Prediction Result: [0.03753179912156057, 0.9624682008784394]",
                        "Features: [12.0, 2.0, 3.0, 4.0]     	Expected Result: 1.0 	Prediction Result: 1.0 	Raw Prediction Result: [0.10041228069167163, 0.8995877193083284]",
                        "Features: [2.0, 2.0, 3.0, 4.0]      	Expected Result: 0.0 	Prediction Result: 0.0 	Raw Prediction Result: [0.9554533965544176, 0.04454660344558237]",
                        "Features: [13.0, 2.0, 3.0, 4.0]     	Expected Result: 1.0 	Prediction Result: 1.0 	Raw Prediction Result: [0.061891528893188164, 0.9381084711068118]",
                        "Features: [4.0, 2.0, 3.0, 4.0]      	Expected Result: 0.0 	Prediction Result: 0.0 	Raw Prediction Result: [0.8822580948141716, 0.11774190518582839]",
                        "Features: [11.0, 2.0, 3.0, 4.0]     	Expected Result: 1.0 	Prediction Result: 1.0 	Raw Prediction Result: [0.15884837044317868, 0.8411516295568213]",
                        "Features: [15.0, 2.0, 3.0, 4.0]     	Expected Result: 1.0 	Prediction Result: 1.0 	Raw Prediction Result: [0.022529496926532833, 0.9774705030734672]",
                        "Features: [5.0, 2.0, 3.0, 4.0]      	Expected Result: 0.0 	Prediction Result: 0.0 	Raw Prediction Result: [0.8158018538556746, 0.1841981461443254]",
                        "Features: [3.0, 2.0, 3.0, 4.0]      	Expected Result: 0.0 	Prediction Result: 0.0 	Raw Prediction Result: [0.926886620226911, 0.07311337977308896]",
                        "Features: [1.0, 2.0, 3.0, 4.0]      	Expected Result: 0.0 	Prediction Result: 0.0 	Raw Prediction Result: [0.9731815427669942, 0.0268184572330058]");

        LogisticRegressionExample.main(new String[0]);
        assertTrue(
                CollectionUtils.isEqualCollection(
                        expectedOutput, Arrays.asList(getOutputString().split("\n"))));
    }
}
