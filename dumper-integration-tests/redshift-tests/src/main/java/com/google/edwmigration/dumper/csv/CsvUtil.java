/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.edwmigration.dumper.csv;

import static com.google.edwmigration.dumper.base.TestConstants.TRAILING_SPACES_REGEX;
import static java.lang.Integer.parseInt;

/** A helper class for reading and extracting data from CSV files. */
public final class CsvUtil {

  private CsvUtil() {}

  /**
   * @return String or an empty string if null.
   */
  public static String getStringNotNull(String value) {
    return value == null ? "" : TRAILING_SPACES_REGEX.matcher(value).replaceFirst("");
  }

  /**
   * @return int or 0 if "".
   */
  public static int getIntNotNull(String value) {
    return getStringNotNull(value).equals("") ? 0 : parseInt(value);
  }

  public static boolean getBooleanNotNull(String value) {
    return getStringNotNull(value).equals("") ? false : Boolean.parseBoolean(value);
  }
}