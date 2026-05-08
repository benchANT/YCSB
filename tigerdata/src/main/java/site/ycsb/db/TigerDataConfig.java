package site.ycsb.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import site.ycsb.DBException;
import site.ycsb.util.PropertyParser;

public final class TigerDataConfig {
  /** The class to use as the jdbc driver. */
  public static final String DRIVER_CLASS = "db.driver";

  /** The URL to connect to the database. */
  public static final String CONNECTION_URL = "db.url";

  /** The user name to use to connect to the database. */
  public static final String CONNECTION_USER = "db.user";

  /** The password to use for establishing the connection. */
  public static final String CONNECTION_PASSWD = "db.passwd";

  /** The batch size for batched inserts. Set to >0 to use batching */
  public static final String DB_BATCH_SIZE = "db.batchsize";

  /** The JDBC fetch size hinted to the driver. */
  public static final String JDBC_FETCH_SIZE = "jdbc.fetchsize";

  /** The JDBC connection auto-commit property for the driver. */
  public static final String JDBC_AUTO_COMMIT = "jdbc.autocommit";

  public static final String JDBC_BATCH_UPDATES = "jdbc.batchupdateapi";

  public static final String JDBC_CREATE_SCHEMA = "jdbc.createschema";

  public static final String JDBC_REFRESH_VIEWS = "jdbc.refreshviews";

  /** The name of the property for the number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /** Default number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  /** Representing a NULL value. */
  public static final String NULL_VALUE = "NULL";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_PREFIX = "FIELD";

  Connection connection;
  boolean initialized = false;
  Properties props;
  int batchSize;
  int jdbcFetchSize;
  boolean batchUpdates;
  boolean createSchema;
  boolean refreshViews;
  boolean autoCommit;
  static final String DEFAULT_PROP = "";
  boolean doDebug = false;
  
    public void init(Properties props) throws DBException {
    if (initialized) {
      System.err.println("Client connection already initialized.");
      return;
    }

    String url = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
    String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
    String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
    String driver = props.getProperty(DRIVER_CLASS);

    doDebug = Boolean.parseBoolean(props.getProperty("debug", "false"));

    this.jdbcFetchSize = PropertyParser.parseIntProperty(props, JDBC_FETCH_SIZE, -1);
    this.batchSize = PropertyParser.parseIntProperty(props, DB_BATCH_SIZE, -1);

    this.autoCommit = PropertyParser.parseBooleanProperty(props, JDBC_AUTO_COMMIT, true);
    this.createSchema = PropertyParser.parseBooleanProperty(props, JDBC_CREATE_SCHEMA, false);
    this.batchUpdates = PropertyParser.parseBooleanProperty(props, JDBC_BATCH_UPDATES, false);
    this.refreshViews = PropertyParser.parseBooleanProperty(props, JDBC_REFRESH_VIEWS, false);

    try {
      connection = DriverManager.getConnection(url, user, passwd);
      connection.setAutoCommit(autoCommit);
      // for a longer explanation see the README.md
      // semicolons aren't present in JDBC urls, so we use them to delimit
      // multiple JDBC connections to shard across.
    } /* catch (ClassNotFoundException e) {
      System.err.println("Error in initializing the JDBS driver: " + e);
      throw new DBException(e);
    } */catch (SQLException e) {
      System.err.println("Error in database operation: " + e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      System.err.println("Invalid value for fieldcount property. " + e);
      throw new DBException(e);
    }
  }

  public void cleanup() throws DBException {
    // nothing to do but closing connection
    try {
      connection.close();
    } catch (SQLException e) {
      System.err.println("Error in database operation: " + e);
      throw new DBException(e);
    }
  }
  
}
