package com.fordevs.dynamicqueryengine.connector;

import com.fordevs.dynamicqueryengine.dto.DatabaseCredentials;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public interface DatabaseConnector {

    Connection connect(DatabaseCredentials credentials) throws SQLException;

    List<String> listTables(Connection connection) throws SQLException;

    List<String> listColumns(Connection connection, String tableName) throws SQLException;

    ResultSet executeQuery(Connection connection, String query) throws SQLException;

    void closeConnection(Connection connection) throws SQLException;
}
