package site.ycsb.db;

import okhttp3.*;
import org.json.JSONArray;
import org.json.JSONObject;
import site.ycsb.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A database interface layer for HarperDB.
 */
public class HarperDBClient extends DB {
  public static final String USERNAME_PROPERTY = "harperdb.username";
  public static final String PASSWORD_PROPERTY = "harperdb.password";
  public static final String DBNAME_PROPERTY = "harperdb.dbname";
  public static final String DBNAME_PROPERTY_DEFAULT = "dev";

  public static final String BATCHSIZE_PROPERTY = "harperdb.batchsize";


  public static final String PORT_PROPERTY = "port";
  public static final String PORT_PROPERTY_DEFAULT = "9925";

  private static final OkHttpClient CLIENT = new OkHttpClient().newBuilder().build();
  private static final MediaType MEDIA_TYPE = MediaType.parse("application/json");
  private static boolean debug = false;

  private static JSONObject tokenObject;
  private static String url;
  private static String dbname;
  private static int batchSize;
  private final List<String> batchInserts = new ArrayList<String>();

  // Used to ensure that only one schema and table are created
  private static final AtomicBoolean DB_INIT_COMPLETE = new AtomicBoolean();

  @Override
  public void init() throws DBException {
    synchronized (DB_INIT_COMPLETE) {
      if (!DB_INIT_COMPLETE.get()) {
        Properties properties = getProperties();

        url = properties.getProperty("harperdb.url", null);
        String port = properties.getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);
        if (url == null) {
          url = "http://localhost:" + port;
        } else {
          url += ":" + port;
        }

        batchSize = Integer.parseInt(properties.getProperty(BATCHSIZE_PROPERTY, "1"));
        String username = properties.getProperty(USERNAME_PROPERTY);
        String password = properties.getProperty(PASSWORD_PROPERTY);
        debug = Boolean.parseBoolean(properties.getProperty("debug", "false"));
        dbname = properties.getProperty(DBNAME_PROPERTY, DBNAME_PROPERTY_DEFAULT);

        if (username == null) {
          username = "HDB_ADMIN";
        }
        if (password == null) {
          password = "password";
        }

        RequestBody tokenBody = RequestBody.create(MEDIA_TYPE, "{\n    \"operation\": " +
            "\"create_authentication_tokens\",\n    \"username\": \"" + username + "\",\n" +
            " \"password\": \"" + password + "\"\n}");

        Request tokenRequest = new Request.Builder()
            .url(url)
            .method("POST", tokenBody)
            .addHeader("Content-Type", "application/json")
            .build();

        try (Response tokenResponse = CLIENT.newCall(tokenRequest).execute()) {
          if (tokenResponse.isSuccessful()) {
            tokenObject = new JSONObject(Objects.requireNonNull(tokenResponse.body()).string());
          } else {
            System.err.println(Objects.requireNonNull(tokenResponse.body()).string());
          }
          RequestBody body = RequestBody.create(MEDIA_TYPE, "{\n    \"operation\": \"create_schema\",\n" +
              "    \"schema\": \"" + dbname + "\"\n}");

          RequestBody createTableBody = RequestBody.create(MEDIA_TYPE, "{\n    \"operation\": \"create_table\",\n" +
              "    \"schema\": \"" + dbname + "\",\n   \"table\": \"usertable\",\n    \"hash_attribute\": \"id\"\n}");

          Request request = requestBuilder(body);
          CLIENT.newCall(request).execute().close();

          request = requestBuilder(createTableBody);
          Response response = CLIENT.newCall(request).execute();
          if (response.isSuccessful()) {
            DB_INIT_COMPLETE.set(true);
            System.out.println("Successfully created connection with " + url);
          } else {
            System.err.println(Objects.requireNonNull(response.body()).string());
          }
          response.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String attributes = attributesBuilder(fields);

    RequestBody readBody = RequestBody.create(MEDIA_TYPE, "{\n    \"operation\": " +
        "\"search_by_hash\",\n    \"schema\": \"" + dbname + "\",\n \"table\": \"usertable\",\n" +
        "\"hash_values\": [\n\"" + key + "\"\n],\n\"get_attributes\": [\n " + attributes + "}");
    Request request = requestBuilder(readBody);

    try (Response response = CLIENT.newCall(request).execute()) {
      if (response.isSuccessful()) {
        if (debug) {
          JSONArray jsonArray = new JSONArray(Objects.requireNonNull(response.body()).string());
          fillMap(result, jsonArray);
        }
        return Status.OK;
      } else {
        System.err.println(Objects.requireNonNull(response.body()).string());
        return Status.ERROR;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /* TODO find an alternative to between, as the startkey and endkey are strings e.g "user123" and between expects int
   * therefore fails sometimes, e.g. for "user8", "user11"
   * Currently not supported
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,
      ByteIterator>> result) {

    String attributes = attributesBuilder(fields);
    String regex = "([a-zA-Z]+)([0-9]+)";
    Matcher matcher = Pattern.compile(regex).matcher(startkey);
    String endkey = "";
    if (matcher.find()) {
      endkey = matcher.group(1) + (Integer.parseInt(matcher.group(2)) + recordcount - 1);
    }
    String condition = "\"search_attribute\": \"id\",\n\"search_type\": \"between\",\n\"search_value\": [\n \""
        + startkey + "\",\n\"" + endkey + "\"\n ]\n}\n]\n";

    RequestBody scanBody = RequestBody.create(MEDIA_TYPE, "{\n    \"operation\": " +
        "\"search_by_conditions\",\n    \"schema\": \"" + dbname + "\",\n \"table\": \"usertable\",\n" +
        "\"limit\": " + recordcount + ",\n\"get_attributes\": [\n " + attributes +
        ",\n \"conditions\": [\n  {\n" + condition + "}");
    Request request = requestBuilder(scanBody);

    try (Response response = CLIENT.newCall(request).execute()) {
      if (response.isSuccessful()) {
        if (debug) {
          JSONArray jsonArray = new JSONArray(Objects.requireNonNull(response.body()).string());
          for (int i = 0; i < jsonArray.length(); i++) {
            HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
            fillMap(resultMap, jsonArray);
            result.add(resultMap);
          }
        }
        return Status.OK;
      } else {
        System.err.println(Objects.requireNonNull(response.body()).string());
        return Status.ERROR;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String records = recordsBuilder(key, values);
    records = "[\n" + records + "\n]";
    RequestBody updateBody = RequestBody.create(MEDIA_TYPE, "{\n    \"operation\": " +
        "\"update\",\n    \"schema\": \"" + dbname + "\",\n \"table\": \"usertable\",\n" +
        "\"records\": " + records + "}");
    Request request = requestBuilder(updateBody);

    try (Response response = CLIENT.newCall(request).execute()) {
      if (response.isSuccessful()) {
        return Status.OK;
      } else {
        System.err.println(Objects.requireNonNull(response.body()).string());
        return Status.ERROR;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String records = "[\n";
    if (batchSize == 1) {
      records += recordsBuilder(key, values);
      records += "\n]";
    } else {
      batchInserts.add(recordsBuilder(key, values));
      if (batchInserts.size() == batchSize) {
        StringJoiner batchRecord = new StringJoiner(",\n");
        for (String record : batchInserts) {
          batchRecord.add(record);
        }
        records += batchRecord + "\n]";
        batchInserts.clear();
      } else {
        return Status.BATCHED_OK;
      }
    }
    RequestBody insertBody = RequestBody.create(MEDIA_TYPE, "{\n    \"operation\": " +
        "\"insert\",\n    \"schema\": \"" + dbname + "\",\n \"table\": \"usertable\",\n" +
        "\"records\": " + records + "}");
    Request request = requestBuilder(insertBody);

    try (Response response = CLIENT.newCall(request).execute()) {
      if (response.isSuccessful()) {
        return Status.OK;
      } else {
        System.err.println(Objects.requireNonNull(response.body()).string());
        return Status.ERROR;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    RequestBody deleteBody = RequestBody.create(MEDIA_TYPE, "{\n    \"operation\": " +
        "\"delete\",\n    \"schema\": \"" + dbname + "\",\n \"table\": \"usertable\",\n" +
        "\"hash_values\": " + "[\n\"" + key + "\"\n" + "]\n" + "}");
    Request request = requestBuilder(deleteBody);

    try (Response response = CLIENT.newCall(request).execute()) {
      if (response.isSuccessful()) {
        return Status.OK;
      } else {
        System.err.println(Objects.requireNonNull(response.body()).string());
        return Status.ERROR;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  protected void fillMap(Map<String, ByteIterator> resultMap, JSONArray jsonArray) {
    for (Object o : jsonArray) {
      JSONObject jsonLineItem = (JSONObject) o;
      for (String key : jsonLineItem.keySet()) {
        if (key.equals("id") || key.contains("field")) {
          resultMap.put(key, new StringByteIterator(jsonLineItem.get(key).toString()));
          System.out.println("Key: " + key + " Value: " + jsonLineItem.get(key).toString());
        }
      }
    }
  }

  private String recordsBuilder(String key, Map<String, ByteIterator> values) {
    StringBuilder records = new StringBuilder(" {\n   \"id\":" + "\"" + key + "\"" + ",\n");
    int i = 0;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      i++;
      String value = entry.getValue().toString();
      // Escape backslash and double quotation marks to get valid json
      value = value.replaceAll("\\\\", "\\\\\\\\");
      value = value.replaceAll("\"", "\\\\\"");
      if (i < values.size()) {
        records.append("    \"").append(entry.getKey()).append("\": \"").append(value).append("\",\n");
      } else {
        records.append("    \"").append(entry.getKey()).append("\": \"").append(value).append("\"\n");
      }
    }
    records.append("    }");
    return records.toString();
  }

  private String attributesBuilder(Set<String> fields) {
    StringBuilder attributes = new StringBuilder();
    if (fields != null) {
      int i = 0;
      for (String field : fields) {
        i++;
        if (i < field.length()) {
          attributes.append("\"").append(field).append("\",\n");
        } else {
          attributes.append("\"").append(field).append("\"\n]\n");
        }
      }
    } else {
      attributes = new StringBuilder("\"*\"\n]");
    }
    return attributes.toString();
  }

  private Request requestBuilder(RequestBody body) {
    return new Request.Builder()
        .url(url)
        .method("POST", body)
        .addHeader("Content-Type", "application/json")
        .addHeader("Authorization", "Bearer " + tokenObject.get("operation_token"))
        .build();
  }

}


