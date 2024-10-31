/**
 * Copyright (c) 2024 benchANT GmbH. All rights reserved.
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
package site.ycsb.workloads;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.IAerospikeQueryDB;
import site.ycsb.NumericByteIterator;
import site.ycsb.Utils;
import site.ycsb.WorkloadException;
import site.ycsb.datamodel.DataModelRegistry;
import site.ycsb.datamodel.DataType;
import site.ycsb.generator.NumberGenerator;


public final class AerospikeWorkload extends CoreWorkload {

  /**
   * The default proportion of transactions that query by secondary index.
   */
  public static final String SIQUERY_PROPORTION_PROPERTY_DEFAULT = "0.05";

  /**
   * The name of the property for the proportion of transactions that are queries by secondary index.
   */
  public static final String SIQUERY_PROPORTION_PROPERTY = "queryproportion";

  /**
  * SI Field name prefix.
  */
  public static final String SI_FIELD_NAME_PREFIX = "sifieldnameprefix";
 /**
  * Default value of the field name prefix.
  */
  public static final String SI_FIELD_NAME_PREFIX_DEFAULT = "sifield";
  /**
  * SI type.
  */
  public static final String SI_FIELD_TYPE_PROPERTY = "sifieldtype";
  /**
  * SI type default.
  */
  public static final String SI_FIELD_TYPE_PROPERTY_DEFAULT = DataType.NUMERIC.toString();
    /**
  * SI Field name prefix.
  */
  public static final String DISABLE_SI_HASH = "disablesihash";
 /**
  * Default value of the field name prefix.
  */
  public static final String DISABLE_SI_HASH_DEFAULT = "false";

  protected NumberGenerator sifieldchooser;
  private final String siFieldName = SI_FIELD_NAME_PREFIX_DEFAULT + 0;
  protected boolean disableHash;
  /**
  * Builds values for all fields.
  */
  @Override
  protected HashMap<String, ByteIterator> buildValues(long keyVal, String key) {
    HashMap<String, ByteIterator> ret = super.buildValues(keyVal, key);
    long indexVal = disableHash ? keyVal : Utils.hash(keyVal);
    ret.put(siFieldName, new NumericByteIterator(indexVal));
    return ret;
  }

   /**
   * Builds a value for a randomly chosen field.
   */
  @Override
  protected HashMap<String, ByteIterator> buildSingleValue(long keyVal, String key) {
    // first: decide if only a regular field shall be updated
    // for now, we only go for regular fields
    final boolean updateRegularField = true;
    if(updateRegularField) {
        return super.buildSingleValue(keyVal, key);
    }
    HashMap<String, ByteIterator> value = new HashMap<>();
    /*
    long indexVal = Utils.hash(keyVal);
    int rotation = 4;
    indexVal = Long.rotateLeft(indexVal, rotation);
    value.put(siFieldName, new NumericByteIterator(indexVal));
    */
    return value;
  }

    /**
     * Initialize the scenario.
     * Called once, in the main client thread, before any operations are started.
     */
    @Override
    public void init(Properties p) throws WorkloadException {
      System.err.println("initializing workload: " + this.getClass().getCanonicalName());
        super.init(p);
        updateOperationGenerator(p);
        // TODO: make sifieldcount configurable
        final String fieldnameprefix = p.getProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);
        final String sifieldnameprefix = p.getProperty(SI_FIELD_NAME_PREFIX, SI_FIELD_NAME_PREFIX_DEFAULT);
        if(sifieldnameprefix.equals(fieldnameprefix)) {
          throw new IllegalStateException("field names and secondary indexs fields cannot have the same prefix");
        }
        for(String f : this.fieldnames) {
          DataModelRegistry.INSTANCE.addRegularField(f);
        }
        // TODO: make configurable if siField is used as a SI
        DataModelRegistry.INSTANCE.addField(siFieldName, true, DataType.NUMERIC);
        disableHash = Boolean.getBoolean(p.getProperty(DISABLE_SI_HASH, DISABLE_SI_HASH_DEFAULT));
        System.err.println("Aerospike workload, SI hashing disabled: " + disableHash);
    }

    @Override
    public final void doTransactionRead(DB db) {
      super.doTransactionRead(db);
    }

    @Override
    public final void doTransactionInsert(DB db) {
        super.doTransactionInsert(db);
    }

    @Override
    public final void doTransactionUpdate(DB db) {
      // pay attention
      // a) when updatesinglefile is selected, the SI field will never be updated
      super.doTransactionUpdate(db);
    }

    public final void doTransactionQuery(DB db) {
      // choose a random key
      long keynum = nextKeynum();
      long indexVal = disableHash ? keynum : Utils.hash(keynum);
      // will fail miserably when db does not have the right type
      IAerospikeQueryDB queryDb = (IAerospikeQueryDB) db;
      HashMap<String, ByteIterator> params = new HashMap<String, ByteIterator>();
      params.put(siFieldName, new NumericByteIterator(indexVal));
      List<Map<String, ByteIterator>> cells = new ArrayList<Map<String, ByteIterator>>();
      queryDb.query(table, params, cells);
    }

    /**
   * Do one transaction operation. Because it will be called concurrently from multiple client
   * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if(operation == null) {
      return false;
    }

    switch (operation) {
    case "READ":
      doTransactionRead(db);
      break;
    case "UPDATE":
      doTransactionUpdate(db);
      break;
    case "INSERT":
      doTransactionInsert(db);
      break;
    case "SCAN":
      throw new UnsupportedOperationException("scan currently not supported");
    case "QUERY":
      doTransactionQuery(db);
      break;
    case "DELETE":
      doTransactionDelete(db);
      break;
    default:
        throw new UnsupportedOperationException("no default operation available");
    }

    return true;
  }

  protected void updateOperationGenerator(final Properties p) {
    if (p == null) {
      throw new IllegalArgumentException("Properties object cannot be null");
    }
    final double queryproportion = Double.parseDouble(
        p.getProperty(SIQUERY_PROPORTION_PROPERTY, SIQUERY_PROPORTION_PROPERTY_DEFAULT));
    final double readmodifywriteproportion = Double.parseDouble(p.getProperty(
        READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));
    if (readmodifywriteproportion > 0) {
        throw new UnsupportedOperationException("read modify write not supported");
    }
    if (queryproportion > 0) {
      operationchooser.addValue(queryproportion, "QUERY");
    }
  }
}
