/**
 * Copyright (c) 2024 YCSB contributors. All rights reserved.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package site.ycsb.db;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.common.base.Strings;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

/**
 * Idiomatic Java client for Google Firestore client for YCSB framework.
 */
public class GoogleFirestoreClient extends site.ycsb.DB {

  /**
   * Property names for the CLI.
   */
  private static final String PROP_PREFIX = "googlefirestore";

  private static final String DEBUG_KEY = "debug";
  private static final String PROJECT_KEY = PROP_PREFIX + ".project";
  private static final String DATABASE_KEY = PROP_PREFIX + ".database";
  private static final String ENDPOINT_KEY = PROP_PREFIX + ".endpoint";
  private static final String CREDENTIALS_KEY = PROP_PREFIX + ".credentials";

  /**
   * Print debug information to standard out.
   */
  private static boolean debug = false;

  /**
   * Tracks running thread counts so we know when to close the session.
   */
  private static int clientRefCount = 0;

  /**
   * Global Firestore client object.
   */
  private static Firestore firestore;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    try {
      globalInit(props);
    } catch (Exception e) {
      throw new DBException("Failed to initialize Firestore client", e);
    }
  }

  private static synchronized void globalInit(Properties props) throws IOException {
    clientRefCount++;
    if (clientRefCount > 1) {
      return;
    }

    debug = Boolean.parseBoolean(props.getProperty(DEBUG_KEY, "false"));

    // Build Firestore options
    FirestoreOptions.Builder builder = FirestoreOptions.newBuilder()
        .setProjectId(getRequiredProp(props, PROJECT_KEY));

    // Optional: Service account credentials file path.
    // If not provided, falls back to Application Default Credentials
    // (GOOGLE_APPLICATION_CREDENTIALS env var or gcloud auth).
    Optional.ofNullable(props.getProperty(CREDENTIALS_KEY))
        .filter(s -> !s.isEmpty())
        .ifPresent(credentialsPath -> {
          try {
            ServiceAccountCredentials credentials =
                ServiceAccountCredentials.fromStream(new FileInputStream(credentialsPath));
            builder.setCredentials(credentials);
            if (debug) {
              System.out.println("Using service account credentials from: " + credentialsPath);
            }
          } catch (IOException e) {
            throw new IllegalStateException(
                "Failed to load credentials from: " + credentialsPath, e);
          }
        });

    // Optional: Database name (defaults to "(default)" if not specified)
    Optional.ofNullable(props.getProperty(DATABASE_KEY))
        .ifPresent(builder::setDatabaseId);

    // Optional: Custom endpoint (useful for emulator)
    Optional<String> emulatorHost =
        Optional.ofNullable(System.getenv().get("FIRESTORE_EMULATOR_HOST"));
    Optional<String> endpoint = Optional.ofNullable(props.getProperty(ENDPOINT_KEY));

    if (emulatorHost.isPresent()) {
      // Emulator host format: "host:port"
      String emulator = emulatorHost.get();
      if (debug) {
        System.out.println("Using Firestore emulator: " + emulator);
      }
      // When using emulator, the endpoint is automatically configured
    } else {
      endpoint.ifPresent(builder::setHost);
    }

    FirestoreOptions options = builder.build();

    if (debug) {
      System.out.println("Initializing Firestore client with settings:");
      System.out.println("  Project: " + options.getProjectId());
      System.out.println("  Database: " + options.getDatabaseId());
      System.out.println("  Host: " + options.getHost());
    }

    firestore = options.getService();
  }

  private static String getRequiredProp(Properties props, String key) {
    String val = props.getProperty(key);
    if (Strings.isNullOrEmpty(val)) {
      throw new IllegalStateException("Missing required property: " + key);
    }
    return val;
  }

  @Override
  public void cleanup() throws DBException {
    synchronized (GoogleFirestoreClient.class) {
      clientRefCount--;
      if (clientRefCount <= 0 && firestore != null) {
        try {
          firestore.close();
          firestore = null;
        } catch (Exception e) {
          throw new DBException("Error closing Firestore client", e);
        }
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (debug) {
      System.out.println("Doing read for key: " + key);
    }

    try {
      // Get the document from Firestore
      ApiFuture<DocumentSnapshot> future =
          firestore.collection(table).document(key).get();

      DocumentSnapshot document = future.get();

      if (debug) {
        System.out.println("Result document exists: " + document.exists());
      }

      // Check if document exists
      if (!document.exists()) {
        return Status.NOT_FOUND;
      }

      // Convert document to map
      Map<String, Object> data = document.getData();
      if (data == null) {
        return Status.NOT_FOUND;
      }

      // Filter fields if specified, otherwise return all fields
      if (fields == null || fields.isEmpty()) {
        // Return all fields
        for (Map.Entry<String, Object> entry : data.entrySet()) {
          result.put(entry.getKey(), new StringByteIterator(entry.getValue().toString()));
        }
      } else {
        // Return only requested fields
        for (String field : fields) {
          Object value = data.get(field);
          if (value != null) {
            result.put(field, new StringByteIterator(value.toString()));
          }
        }
      }

      if (debug) {
        System.out.println("Read completed for key: " + key + ", fields: " + result.size());
      }

      return Status.OK;
    } catch (InterruptedException e) {
      if (debug) {
        System.err.println("Read interrupted for key: " + key);
        e.printStackTrace();
      }
      Thread.currentThread().interrupt();
      return Status.ERROR;
    } catch (ExecutionException e) {
      if (debug) {
        System.err.println("Read failed for key: " + key);
        e.printStackTrace();
      }
      return Status.ERROR;
    } catch (Exception e) {
      if (debug) {
        System.err.println("Unexpected error during read for key: " + key);
        e.printStackTrace();
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return null;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    if (debug) {
      System.out.println("Doing update for key: " + key);
    }

    try {
      // Convert ByteIterator values to strings for Firestore document
      Map<String, Object> data = new HashMap<>();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        data.put(entry.getKey(), entry.getValue().toString());
      }

      // Update the document in Firestore (creates if doesn't exist due to set with merge)
      ApiFuture<com.google.cloud.firestore.WriteResult> future =
          firestore.collection(table).document(key).set(data);

      // Wait for the operation to complete
      future.get();

      if (debug) {
        System.out.println("Update completed for key: " + key);
      }

      return Status.OK;
    } catch (InterruptedException e) {
      if (debug) {
        System.err.println("Update interrupted for key: " + key);
        e.printStackTrace();
      }
      Thread.currentThread().interrupt();
      return Status.ERROR;
    } catch (ExecutionException e) {
      if (debug) {
        System.err.println("Update failed for key: " + key);
        e.printStackTrace();
      }
      return Status.ERROR;
    } catch (Exception e) {
      if (debug) {
        System.err.println("Unexpected error during update for key: " + key);
        e.printStackTrace();
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    if (debug) {
      System.out.println("Doing insert for key: " + key);
    }

    try {
      // Convert ByteIterator values to strings for Firestore document
      Map<String, Object> data = new HashMap<>();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        data.put(entry.getKey(), entry.getValue().toString());
      }

      // Set the document in Firestore (creates if doesn't exist)
      ApiFuture<com.google.cloud.firestore.WriteResult> future =
          firestore.collection(table).document(key).set(data);

      // Wait for the operation to complete
      future.get();

      if (debug) {
        System.out.println("Insert completed for key: " + key);
      }

      return Status.OK;
    } catch (InterruptedException e) {
      if (debug) {
        System.err.println("Insert interrupted for key: " + key);
        e.printStackTrace();
      }
      Thread.currentThread().interrupt();
      return Status.ERROR;
    } catch (ExecutionException e) {
      if (debug) {
        System.err.println("Insert failed for key: " + key);
        e.printStackTrace();
      }
      return Status.ERROR;
    } catch (Exception e) {
      if (debug) {
        System.err.println("Unexpected error during insert for key: " + key);
        e.printStackTrace();
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    if (debug) {
      System.out.println("Doing delete for key: " + key);
    }

    try {
      // Delete the document from Firestore
      ApiFuture<com.google.cloud.firestore.WriteResult> future =
          firestore.collection(table).document(key).delete();

      // Wait for the operation to complete
      future.get();

      if (debug) {
        System.out.println("Delete completed for key: " + key);
      }

      return Status.OK;
    } catch (InterruptedException e) {
      if (debug) {
        System.err.println("Delete interrupted for key: " + key);
        e.printStackTrace();
      }
      Thread.currentThread().interrupt();
      return Status.ERROR;
    } catch (ExecutionException e) {
      if (debug) {
        System.err.println("Delete failed for key: " + key);
        e.printStackTrace();
      }
      return Status.ERROR;
    } catch (Exception e) {
      if (debug) {
        System.err.println("Unexpected error during delete for key: " + key);
        e.printStackTrace();
      }
      return Status.ERROR;
    }
  }
}
