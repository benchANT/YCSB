
/**
 * Copyright (c) 2010 Yahoo! Inc., 2016-2020 YCSB contributors. All rights reserved.
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

package com.benchant.realtimeiot;

import site.ycsb.measurements.Measurements;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import com.benchant.realtimeiot.data.RtIotMachineData;
import com.benchant.realtimeiot.data.RtIotOperatorData;
import com.benchant.realtimeiot.data.RtIotSensorData;
import com.benchant.realtimeiot.data.RtIotSitesData;

import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wrapper around a "real" Rt DB that measures latencies and counts return codes.
 * Also reports latency separately between OK and failed operations.
 */
public final class RtIotDBWrapper extends RtIotDb {

  private final RtIotDb db;
  private final Measurements measurements;
  private final Tracer tracer;

  private boolean reportLatencyForEachError = false;
  private Set<String> latencyTrackedErrors = new HashSet<String>();

  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY = "reportlatencyforeacherror";
  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT = "false";

  private static final String LATENCY_TRACKED_ERRORS_PROPERTY = "latencytrackederrors";

  private static final AtomicBoolean LOG_REPORT_CONFIG = new AtomicBoolean(false);

  private final String scopeStringCleanup;
  private final String scopeStringInit;
  private final String scopeStringInsertMachines;
  private final String scopeStringInsertSites;
  private final String scopeStringInsertSensors;
  private final String scopeStringInsertOperators;
  private final String scopeQuery;


  public RtIotDBWrapper(final RtIotDb db, final Tracer tracer) {
    this.db = db;
    measurements = Measurements.getMeasurements();
    this.tracer = tracer;
    final String simple = db.getClass().getSimpleName();

    scopeStringCleanup = simple + "#cleanup";
  
    scopeStringInit = simple + "#init";
    scopeStringInsertMachines = simple + "#insertMachines";
    scopeStringInsertSites = simple + "#insertSites";
    scopeStringInsertSensors = simple + "#insertSensors";
    scopeStringInsertOperators = simple + "#insertOperators";
    scopeQuery = simple + "#query";
  }

  /**
   * Set the properties for this DB.
   */
  public void setProperties(Properties p) {
    db.setProperties(p);
  }

  /**
   * Get the set of properties for this DB.
   */
  public Properties getProperties() {
    return db.getProperties();
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    try (final TraceScope span = tracer.newScope(scopeStringInit)) {
      db.init();

      this.reportLatencyForEachError = Boolean.parseBoolean(getProperties().
          getProperty(REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY,
              REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT));

      if (!reportLatencyForEachError) {
        String latencyTrackedErrorsProperty = getProperties().getProperty(LATENCY_TRACKED_ERRORS_PROPERTY, null);
        if (latencyTrackedErrorsProperty != null) {
          this.latencyTrackedErrors = new HashSet<String>(Arrays.asList(
              latencyTrackedErrorsProperty.split(",")));
        }
      }

      if (LOG_REPORT_CONFIG.compareAndSet(false, true)) {
        System.err.println("DBWrapper: report latency for each error is " +
            this.reportLatencyForEachError + " and specific error codes to track" +
            " for latency are: " + this.latencyTrackedErrors.toString());
      }
    }
  }

    private void measure(String op, Status result, long intendedStartTimeNanos,
                       long startTimeNanos, long endTimeNanos) {
    String measurementName = op;
    if (result == null || !result.isOk()) {
      if (this.reportLatencyForEachError ||
          this.latencyTrackedErrors.contains(result.getName())) {
        measurementName = op + "-" + result.getName();
      } else {
        measurementName = op + "-FAILED";
      }
    }
    measurements.measure(measurementName,
        (int) ((endTimeNanos - startTimeNanos) / 1000));
    measurements.measureIntended(measurementName,
        (int) ((endTimeNanos - intendedStartTimeNanos) / 1000));
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {
    try (final TraceScope span = tracer.newScope(scopeStringCleanup)) {
      long ist = measurements.getIntendedStartTimeNs();
      long st = System.nanoTime();
      db.cleanup();
      long en = System.nanoTime();
      measure("CLEANUP", Status.OK, ist, st, en);
    }
  }

    public Status insertMachineData(List<RtIotMachineData> machineData) {
      try (final TraceScope span = tracer.newScope(scopeStringInsertMachines)) {
        long ist = measurements.getIntendedStartTimeNs();
        long st = System.nanoTime();
        Status res = db.insertMachineData(machineData);
        long en = System.nanoTime();
        measure("INSERT-MACHINE", res, ist, st, en);
        measurements.reportStatus("INSERT-MACHINE", res);
        return res;
      }
    }

    public Status insertSiteData(List<RtIotSitesData> siteData) {
      try (final TraceScope span = tracer.newScope(scopeStringInsertSites)) {
        long ist = measurements.getIntendedStartTimeNs();
        long st = System.nanoTime();
        Status res = db.insertSiteData(siteData);
        long en = System.nanoTime();
        measure("INSERT-SITE", res, ist, st, en);
        measurements.reportStatus("INSERT-SITE", res);
        return res;
      }
    }

    public Status insertOperatorData(List<RtIotOperatorData> operatorData) {
      try (final TraceScope span = tracer.newScope(scopeStringInsertOperators)) {
        long ist = measurements.getIntendedStartTimeNs();
        long st = System.nanoTime();
        Status res = db.insertOperatorData(operatorData);
        long en = System.nanoTime();
        measure("INSERT-OPERATOR", res, ist, st, en);
        measurements.reportStatus("INSERT-OPERATOR", res);
        return res;
      }
    }

    public Status insertSensorData(List<RtIotSensorData> sensorData) {
      try (final TraceScope span = tracer.newScope(scopeStringInsertSensors)) {
        long ist = measurements.getIntendedStartTimeNs();
        long st = System.nanoTime();
        Status res = db.insertSensorData(sensorData);
        long en = System.nanoTime();
        measure("INSERT-SENSOR", res, ist, st, en);
        measurements.reportStatus("INSERT-SENSOR", res);
        return res;
      }
    }

  @Override
  public final Status query(String queryId, Map<String, Object> params, List<HashMap<String, ByteIterator>> result){
      try (final TraceScope span = tracer.newScope(scopeQuery+queryId)) {
        long ist = measurements.getIntendedStartTimeNs();
        long st = System.nanoTime();
        Status res = db.query(queryId, params, result);
        long en = System.nanoTime();
        measure("Q" +queryId, res, ist, st, en);
        measurements.reportStatus("Q" + queryId, res);
        return res;
      }
    }
}