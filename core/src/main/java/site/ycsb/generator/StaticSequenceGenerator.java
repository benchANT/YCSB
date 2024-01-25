/*
 * Copyright (c) 2024 benchANT GmbH. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */
package site.ycsb.generator;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public final class StaticSequenceGenerator<T> extends Generator<StaticSequenceGenerator.SequenceItem<T>> {

    public static final class SequenceItem<T> {
        public final int index;
        public final T item;
        SequenceItem(int index, T item) {
            this.index = index;
            this.item = item;
        }
    }

    public static enum SequenceGenerationOrder {
        DEPTH_FIRST,
        BREADTH_FIRST
    }
    private final SequenceGenerationOrder order;
    private final int threshold;
    private final int repetitions;
    private final T[] sequence;
    private final AtomicInteger counter = new AtomicInteger(-1);

    public StaticSequenceGenerator(T[] sequence) {
        this(sequence, 1, SequenceGenerationOrder.DEPTH_FIRST);
    }
    public StaticSequenceGenerator(T[] sequence, int repetitions, SequenceGenerationOrder order) {
        this.order = order;
        this.sequence = Arrays.copyOf(sequence, sequence.length);
        if(repetitions < 1) {
            this.repetitions = 1;
        } else {
            this.repetitions = repetitions;
        }
        threshold = repetitions * this.sequence.length - 1;
    }

    private SequenceItem<T>getValue(int counter) {
        if(counter < 0) { return null ; }
        if(counter > threshold) { return null ; }
        if(order == SequenceGenerationOrder.DEPTH_FIRST) {
            int index = counter / repetitions;
            return new SequenceItem<T>(counter, sequence[index]);
        }
        // BREADTH_FIRST approach
        int index = counter % sequence.length;
        return new SequenceItem<T>(counter, sequence[index]);
    }

    public final SequenceItem<T> lastValue() {
        int val = counter.get();
        return getValue(val);
    }

    public final SequenceItem<T> nextValue() {
        int val = counter.incrementAndGet();
        return getValue(val);
    }
}