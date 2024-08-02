package com.fordevs.dynamicqueryengine.service;

import com.fordevs.dynamicqueryengine.config.DynamicDataSourceManager;
import com.fordevs.dynamicqueryengine.dto.DatabaseCredentials;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Service
public class DatabaseServiceImpl implements DatabaseService {

    @Autowired
    private DynamicDataSourceManager dataSourceManager;

    @Override
    public Connection connect(DatabaseCredentials credentials) throws SQLException {
        // Use the databaseType directly from the credentials
        return dataSourceManager.connect(credentials);
    }

    @Override
    public List<String> listTables(Connection connection) throws SQLException {
        return dataSourceManager.listTables(getDatabaseTypeFromConnection(connection), connection);
    }

    @Override
    public List<String> listColumns(Connection connection, String tableName) throws SQLException {
        return dataSourceManager.listColumns(getDatabaseTypeFromConnection(connection), connection, tableName);
    }

    @Override
    public ResultSet executeQuery(Connection connection, String query) throws SQLException {
        return dataSourceManager.executeQuery(getDatabaseTypeFromConnection(connection), connection, query);
    }

    @Override
    public void closeConnection(Connection connection) throws SQLException {
        dataSourceManager.closeConnection(getDatabaseTypeFromConnection(connection), connection);
    }

    // Helper method to extract the database type from the connection metadata
    private String getDatabaseTypeFromConnection(Connection connection) {
        try {
            String dbProductName = connection.getMetaData().getDatabaseProductName().toLowerCase();
            if (dbProductName.contains("postgresql")) {
                return "postgresql";
            }
            // Add more conditions for other database types as needed
        } catch (SQLException e) {
            // Handle exception or return a default value
        }
        return "unknown";
    }
}
