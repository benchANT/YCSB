/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights reserved.
 * Copyright (c) 2023-2024 benchANT GmbH. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.generator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Objects.requireNonNull;

/**
 * Generates a distribution by choosing from a discrete set of values.
 */
public class GenericDiscreteGenerator<T> extends Generator<T> {
  private class Pair {
    private double weight;
    private T value;

    Pair(double weight, T value) {
      this.weight = weight;
      this.value = requireNonNull(value);
    }
  }

  private final Collection<Pair> values = new ArrayList<>();
  private T lastvalue;

  public GenericDiscreteGenerator() {
    lastvalue = null;
  }

  /**
   * Generate the next string in the distribution.
   */
  @Override
  public T nextValue() {
    double sum = 0;

    for (Pair p : values) {
      sum += p.weight;
    }

    double val = ThreadLocalRandom.current().nextDouble();

    for (Pair p : values) {
      double pw = p.weight / sum;
      if (val < pw) {
        return p.value;
      }

      val -= pw;
    }

    throw new AssertionError("oops. should not get here.");

  }

  /**
   * Return the previous string generated by the distribution; e.g., returned from the last nextString() call.
   * Calling lastString() should not advance the distribution or have any side effects. If nextString() has not yet
   * been called, lastString() should return something reasonable.
   */
  @Override
  public T lastValue() {
    if (lastvalue == null) {
      lastvalue = nextValue();
    }
    return lastvalue;
  }

  public void addValue(double weight, T value) {
    values.add(new Pair(weight, value));
  }

}
