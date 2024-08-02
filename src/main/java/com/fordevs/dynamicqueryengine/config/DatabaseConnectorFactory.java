package com.fordevs.dynamicqueryengine.config;

import com.fordevs.dynamicqueryengine.connector.DatabaseConnector;
import com.fordevs.dynamicqueryengine.connector.PostgresConnector;

public class DatabaseConnectorFactory {

    public static DatabaseConnector getConnector(String databaseType) {
        switch (databaseType.toLowerCase()) {
            case "postgresql":
                return new PostgresConnector();
            // Add cases for MySQL, Oracle, SQL Server, etc.
            default:
                throw new IllegalArgumentException("Unsupported database type: " + databaseType);
        }
    }
}
