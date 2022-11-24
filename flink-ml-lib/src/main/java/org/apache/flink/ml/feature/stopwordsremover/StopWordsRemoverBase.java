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

package org.apache.flink.ml.feature.stopwordsremover;

import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Base class of {@link StopWordsRemover} that contains methods and variables that needs to be
 * provided by {@link StopWordsRemover} and also used by {@link StopWordsRemoverParams}.
 */
abstract class StopWordsRemoverBase {

    private static final Set<String> SUPPORTED_LANGUAGES =
            new HashSet<>(
                    Arrays.asList(
                            "danish",
                            "dutch",
                            "english",
                            "finnish",
                            "french",
                            "german",
                            "hungarian",
                            "italian",
                            "norwegian",
                            "portuguese",
                            "russian",
                            "spanish",
                            "swedish",
                            "turkish"));

    protected static final Logger LOG = LoggerFactory.getLogger(StopWordsRemover.class);

    /**
     * Loads the default stop words for the given language.
     *
     * <p>Supported languages: danish, dutch, english, finnish, french, german, hungarian, italian,
     * norwegian, portuguese, russian, spanish, swedish, turkish
     *
     * @see <a
     *     href="http://anoncvs.postgresql.org/cvsweb.cgi/pgsql/src/backend/snowball/stopwords/">here</a>
     */
    public static String[] loadDefaultStopWords(String language) {
        Preconditions.checkArgument(
                SUPPORTED_LANGUAGES.contains(language),
                "%s is not in the supported language list: %s.",
                language,
                SUPPORTED_LANGUAGES);

        InputStream in =
                StopWordsRemover.class
                        .getClassLoader()
                        .getResourceAsStream(
                                "org/apache/flink/ml/feature/stopwords/" + language + ".txt");

        return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
                .lines()
                .toArray(String[]::new);
    }

    /**
     * Returns system default locale, or {@link Locale#US} if the default locale is not in available
     * locales in JVM. The locale is returned as a String.
     */
    public static String getDefaultOrUS() {
        if (Arrays.asList(Locale.getAvailableLocales()).contains(Locale.getDefault())) {
            return Locale.getDefault().toString();
        } else {
            LOG.warn(
                    "Default locale set was [{}]; however, it was "
                            + "not found in available locales in JVM, falling back to en_US locale. Set param `locale` "
                            + "in order to respect another locale.",
                    Locale.getDefault());
            return Locale.US.toString();
        }
    }
}
