package site.ycsb.db;

import okhttp3.*;
import org.json.JSONArray;
import org.json.JSONObject;
import site.ycsb.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A database interface layer for HarperDB.
 */
public class HarperDBClient extends DB {
  public static final String USERNAME_PROPERTY = "harperdb.username";
  public static final String PASSWORD_PROPERTY = "harperdb.password";
  public static final String DBNAME_PROPERTY = "harperdb.dbname";
  public static final String DBNAME_PROPERTY_DEFAULT = "usertable";


  public static final String PORT_PROPERTY = "port";
  public static final String PORT_PROPERTY_DEFAULT = "9925";

  private static JSONObject tokenObject;
  private static final OkHttpClient CLIENT = new OkHttpClient().newBuilder().build();
  private static final MediaType MEDIA_TYPE = MediaType.parse("application/json");
  private static String url;
  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  @Override
  public void init() throws DBException {

    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    // Synchronized so that we only have a single
    // cluster/session instance for all the threads.
    synchronized (INIT_COUNT) {
      Properties properties = getProperties();

      url = properties.getProperty("harperdb.url", null);
      String port = getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);
      if (url == null) {
        url = "http://localhost:" + port;
      } else {
        url += ":" + port;
      }

      String username = getProperties().getProperty(USERNAME_PROPERTY);
      String password = getProperties().getProperty(PASSWORD_PROPERTY);

      String tablename = getProperties().getProperty(DBNAME_PROPERTY, DBNAME_PROPERTY_DEFAULT);

      if (username == null) {
        username = "HDB_ADMIN";
      }
      if (password == null) {
        password = "password";
      }

      RequestBody body = RequestBody.create(MEDIA_TYPE, "{\n    \"operation\": \"create_schema\",\n" +
          "    \"schema\": \"dev\"\n}");

      RequestBody createTableBody = RequestBody.create(MEDIA_TYPE, "{\n    \"operation\": \"create_table\",\n" +
          "    \"schema\": \"dev\",\n   \"table\": \"" + tablename + "\",\n    \"hash_attribute\": \"id\"\n}");

      RequestBody tokenBody = RequestBody.create(MEDIA_TYPE, "{\n    \"operation\": " +
          "\"create_authentication_tokens\",\n    \"username\": \"" + username + "\",\n" +
          " \"password\": \"" + password + "\"\n}");

      Request tokenRequest = new Request.Builder()
          .url(url)
          .method("POST", tokenBody)
          .addHeader("Content-Type", "application/json")
          .build();

      try (Response tokenResponse = CLIENT.newCall(tokenRequest).execute()) {
        tokenObject = new JSONObject(Objects.requireNonNull(tokenResponse.body()).string());
        Request request = requestBuilder(body);
        CLIENT.newCall(request).execute().close();

        request = requestBuilder(createTableBody);
        Response response = CLIENT.newCall(request).execute();
        if (response.isSuccessful()) {
          System.out.println("Successfully created connection with " + url);
        }
        response.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String attributes = attributesBuilder(fields);

    RequestBody readBody = RequestBody.create(MEDIA_TYPE, "{\n    \"operation\": " +
        "\"search_by_hash\",\n    \"schema\": \"dev\",\n \"table\": \"" + table + "\",\n" +
        "\"hash_values\": [\n\"" + key + "\"\n],\n\"get_attributes\": [\n " + attributes + "}");
    Request request = requestBuilder(readBody);

    try (Response response = CLIENT.newCall(request).execute()) {
      if (response.isSuccessful()) {
        JSONArray jsonArray = new JSONArray(Objects.requireNonNull(response.body()).string());
        fillMap(result, jsonArray);
        return Status.OK;
      } else {
        return Status.ERROR;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,
      ByteIterator>> result) {

    String attributes = attributesBuilder(fields);
    String regex = "([a-zA-Z]{1,})([0-9]{1,})";
    Matcher matcher = Pattern.compile(regex).matcher(startkey);
    String endkey = "";
    if (matcher.find()) {
      endkey = matcher.group(1) + (Integer.parseInt(matcher.group(2)) + recordcount);
    }
    String condition = "\"search_attribute\": \"id\",\n\"search_type\": \"between\",\n\"search_value\": [\n \""
        + startkey + "\",\n\"" + endkey + "\"\n ]\n}\n]\n";

    RequestBody scanBody = RequestBody.create(MEDIA_TYPE, "{\n    \"operation\": " +
        "\"search_by_conditions\",\n    \"schema\": \"dev\",\n \"table\": \"" + table + "\",\n" +
        "\"get_attributes\": [\n " + attributes + ",\n \"conditions\": [\n  {\n" + condition + "}");
    Request request = requestBuilder(scanBody);

    try (Response response = CLIENT.newCall(request).execute()) {
      if (response.isSuccessful()) {
        JSONArray jsonArray = new JSONArray(Objects.requireNonNull(response.body()).string());
//        System.out.println("response length: " + jsonArray.length() + " recordcount: " + recordcount
//            + " start: " + startkey);
        for (int i = 0; i < jsonArray.length(); i++) {
          HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
          fillMap(resultMap, jsonArray);
          result.add(resultMap);
        }
        return Status.OK;
      } else {
        return Status.ERROR;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    StringBuilder records = new StringBuilder("[\n  {\n   \"id\":" + "\"" + key + "\"" + ",\n");

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
    records.append("    }\n]\n");

    RequestBody updateBody = RequestBody.create(MEDIA_TYPE, "{\n    \"operation\": " +
        "\"update\",\n    \"schema\": \"dev\",\n \"table\": \"" + table + "\",\n" +
        "\"records\": " + records + "}");
    Request request = requestBuilder(updateBody);

    try (Response response = CLIENT.newCall(request).execute()) {
      if (response.isSuccessful()) {
        return Status.OK;
      } else {
        return Status.ERROR;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {

    StringBuilder records = new StringBuilder("[\n  {\n   \"id\":" + "\"" + key + "\"" + ",\n");

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
    records.append("    }\n]\n");

    RequestBody insertBody = RequestBody.create(MEDIA_TYPE, "{\n    \"operation\": " +
        "\"insert\",\n    \"schema\": \"dev\",\n \"table\": \"" + table + "\",\n" +
        "\"records\": " + records + "}");
    Request request = requestBuilder(insertBody);

    try (Response response = CLIENT.newCall(request).execute()) {
//      System.out.println(response.body().string());
      if (response.isSuccessful()) {
        return Status.OK;
      } else {
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
        "\"delete\",\n    \"schema\": \"dev\",\n \"table\": \"" + table + "\",\n" +
        "\"hash_values\": " + "[\n\"" + key + "\"\n" + "]\n" + "}");
    Request request = requestBuilder(deleteBody);

    try (Response response = CLIENT.newCall(request).execute()) {
      System.out.println("response: " + response.body().string());
      if (response.isSuccessful()) {
        return Status.OK;
      } else {
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
        resultMap.put(key, new StringByteIterator(jsonLineItem.get(key).toString()));
      }
    }
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

