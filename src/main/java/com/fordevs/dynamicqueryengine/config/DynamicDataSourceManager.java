package com.fordevs.dynamicqueryengine.config;

import com.fordevs.dynamicqueryengine.connector.DatabaseConnector;
import com.fordevs.dynamicqueryengine.dto.DatabaseCredentials;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DynamicDataSourceManager {

    private final Map<String, DataSource> dataSources = new HashMap<>();
    private final Map<String, DatabaseConnector> connectors = new HashMap<>();

    public Connection connect(DatabaseCredentials credentials) throws SQLException {
        String databaseType = credentials.getDatabaseType();
        DatabaseConnector connector = getConnector(databaseType, credentials);
        if (connector == null) {
            connector = setConnector(databaseType, credentials);
        }
        return connector.connect(credentials);
    }

    public String getConnectionString(DatabaseCredentials credentials) {
        return String.format("%s:%d/%s",
                credentials.getHost(),
                credentials.getPort(),
                credentials.getDatabaseName());
    }

    private DatabaseConnector getConnector(String databaseType, DatabaseCredentials credentials) {
        String connectionString = getConnectionString(credentials);
        return connectors.get(connectionString);
    }

    private DatabaseConnector setConnector(String databaseType, DatabaseCredentials credentials) {
        String connectionString = getConnectionString(credentials);
        DatabaseConnector connector = DatabaseConnectorFactory.getConnector(databaseType);
        connectors.put(connectionString, connector);
        return connector;
    }

    public JdbcTemplate getJdbcTemplateForDb(String connectionString) {
        DataSource dataSource = dataSources.get(connectionString);
        if (dataSource == null) {
            throw new IllegalArgumentException("No DataSource found for connection string: " + connectionString);
        }
        return new JdbcTemplate(dataSource);
    }

    public List<String> listTables(String databaseType, Connection connection) throws SQLException {
        DatabaseConnector connector = connectors.get(getConnectionStringFromConnection(connection));
        if (connector == null) {
            throw new IllegalArgumentException("No connector found for database type: " + databaseType);
        }
        return connector.listTables(connection);
    }

    public List<String> listColumns(String databaseType, Connection connection, String tableName) throws SQLException {
        DatabaseConnector connector = connectors.get(getConnectionStringFromConnection(connection));
        if (connector == null) {
            throw new IllegalArgumentException("No connector found for database type: " + databaseType);
        }
        return connector.listColumns(connection, tableName);
    }

    public ResultSet executeQuery(String databaseType, Connection connection, String query) throws SQLException {
        DatabaseConnector connector = connectors.get(getConnectionStringFromConnection(connection));
        if (connector == null) {
            throw new IllegalArgumentException("No connector found for database type: " + databaseType);
        }
        return connector.executeQuery(connection, query);
    }

    public void closeConnection(String databaseType, Connection connection) throws SQLException {
        DatabaseConnector connector = connectors.get(getConnectionStringFromConnection(connection));
        if (connector != null) {
            connector.closeConnection(connection);
        }
    }

    private String getConnectionStringFromConnection(Connection connection) throws SQLException {
        return String.format("%s:%d/%s",
                connection.getMetaData().getURL(),
                connection.getMetaData().getDatabaseMajorVersion(),
                connection.getCatalog());
    }

    public void registerDataSource(String connectionString, DataSource dataSource) {
        dataSources.put(connectionString, dataSource);
    }
}
