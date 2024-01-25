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

import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import site.ycsb.workloads.TelegrafWorkload.OperationType;

public class TelegrafDbWrapper extends DBWrapper implements TelegrafDbIfc {
    private final TelegrafDbIfc myDb;
    private final String scopeString;
    public TelegrafDbWrapper(final DB db, final Tracer tracer) {
        super(db, tracer);
        myDb = (TelegrafDbIfc) db;
        scopeString = db.getClass().getSimpleName() + "#";
    }

     @Override
    public Status tsQuery(OperationType ot, Duration timeInterval, Vector<HashMap<String, ByteIterator>> result) {
        // select count(ppid) from procstat where time > now() - <interval>; 
        final String name = ot.name().toUpperCase();
        try (final TraceScope span = tracer.newScope(scopeString + ot.name())) {
            long ist = measurements.getIntendedStartTimeNs();
            long st = System.nanoTime();
            Status res = myDb.tsQuery(ot, timeInterval, result);
            long en = System.nanoTime();
            measure(name, res, ist, st, en);
            measurements.reportStatus(name, res);
            return res;
        }
    }
}