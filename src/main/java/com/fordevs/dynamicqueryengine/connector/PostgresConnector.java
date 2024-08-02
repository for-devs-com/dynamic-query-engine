package com.fordevs.dynamicqueryengine.connector;

import com.fordevs.dynamicqueryengine.dto.DatabaseCredentials;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class PostgresConnector implements DatabaseConnector {

    @Override
    public Connection connect(DatabaseCredentials credentials) throws SQLException {
        String url = String.format("jdbc:postgresql://%s:%d/%s",
                credentials.getHost(),
                credentials.getPort(),
                credentials.getDatabaseName());
        return DriverManager.getConnection(url, credentials.getUserName(), credentials.getPassword());
    }

    @Override
    public List<String> listTables(Connection connection) throws SQLException {
        List<String> tables = new ArrayList<>();
        String query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                tables.add(resultSet.getString("table_name"));
            }
        }
        return tables;
    }

    @Override
    public List<String> listColumns(Connection connection, String tableName) throws SQLException {
        List<String> columns = new ArrayList<>();
        String query = String.format("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", tableName);
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                columns.add(resultSet.getString("column_name"));
            }
        }
        return columns;
    }

    @Override
    public ResultSet executeQuery(Connection connection, String query) throws SQLException {
        Statement statement = connection.createStatement();
        return statement.executeQuery(query);
    }

    @Override
    public void closeConnection(Connection connection) throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}
