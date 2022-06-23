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

package org.apache.flink.ml.examples.evaluation;

import org.apache.flink.ml.examples.ExampleOutputTestBase;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test for {@link BinaryClassificationEvaluatorExample}. */
public class BinaryClassificationEvaluatorExampleTest extends ExampleOutputTestBase {
    @Test
    public void testExample() {
        String expectedOutput =
                "Area under the precision-recall curve: 0.7691481137909708\n"
                        + "Area under the receiver operating characteristic curve: 0.6571428571428571\n"
                        + "Kolmogorov-Smirnov value: 0.3714285714285714\n";

        BinaryClassificationEvaluatorExample.main(new String[0]);
        assertEquals(expectedOutput, getOutputString());
    }
}
