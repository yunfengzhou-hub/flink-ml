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

/** Test for {@link StringIndexerExample}. */
public class StringIndexerExampleTest extends ExampleOutputTestBase {
    @Test
    public void testExample() {
        List<String> expectedOutput =
                Arrays.asList(
                        "Input Values: [c, 2.0] \tOutput Values: [2.0, 3.0]",
                        "Input Values: [b, 1.0] \tOutput Values: [1.0, 2.0]",
                        "Input Values: [a, 2.0] \tOutput Values: [0.0, 3.0]");

        StringIndexerExample.main(new String[0]);
        assertTrue(
                CollectionUtils.isEqualCollection(
                        expectedOutput, Arrays.asList(getOutputString().split("\n"))));
    }
}
