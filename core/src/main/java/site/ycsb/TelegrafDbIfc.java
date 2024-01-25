/*
 * Copyright (c) 2023 - 2024, benchANT GmbH. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb;

import java.time.Duration;
import java.util.HashMap;
import java.util.Vector;

import site.ycsb.workloads.TelegrafWorkload.OperationType;

public interface TelegrafDbIfc {
    public static String Q02_WRITE_BYTES_FILTER = "> 20000";
    public static String Q03_CPU_TIME_FILTER = "> 2";
    public static String Q06_CPU_TIME_FILTER = "> 7";
    public static String Q07_THREADS_FILTER = "> 1";
    public static String Q10_CPU_TIME_FILTER = "> 5";
    public static String Q10_MEMORY_FILTER = ">= 0.05";
    public static String Q11_CPU_TIME_FILTER = "> 8";
    public static String Q12_MEMORY_USAGE_FILTER = "> 0.1";
    public static String Q13_CPU_TIME_FILTER = "> 8";
    public static String Q13_MEMORY_USAGE_FILTER = "> 0.1";
    public static String Q15_WRITE_BYTES_FILTER = "> 20000";

    public abstract Status tsQuery(OperationType op, Duration timeInterval, Vector<HashMap<String, ByteIterator>> result);
}