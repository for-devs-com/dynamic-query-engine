package com.fordevs.dynamicqueryengine.service;

import com.fordevs.dynamicqueryengine.config.DynamicDataSourceManager;
import com.fordevs.dynamicqueryengine.dto.DatabaseCredentials;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class SchemaDiscoveryService {

    private final DynamicDataSourceManager dataSourceManager;

    public SchemaDiscoveryService(DynamicDataSourceManager dataSourceManager) {
        this.dataSourceManager = dataSourceManager;
    }

    public List<String> listTables(DatabaseCredentials credentials) throws SQLException {
        String connectionString = dataSourceManager.getConnectionString(credentials);
        JdbcTemplate jdbcTemplate = dataSourceManager.getJdbcTemplateForDb(connectionString);

        if (jdbcTemplate == null) {
            throw new SQLException("Unable to obtain JdbcTemplate for given credentials.");
        }

        return jdbcTemplate.execute((Connection con) -> {
            List<String> tableList = new ArrayList<>();
            DatabaseMetaData metaData = con.getMetaData();
            try (ResultSet rs = metaData.getTables(null, "public", "%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    tableList.add(rs.getString("TABLE_NAME"));
                }
            }
            return tableList;
        });
    }

    public List<String> listColumns(DatabaseCredentials credentials, String tableName) throws SQLException {
        String connectionString = dataSourceManager.getConnectionString(credentials);
        JdbcTemplate jdbcTemplate = dataSourceManager.getJdbcTemplateForDb(connectionString);

        if (jdbcTemplate == null) {
            throw new SQLException("Unable to obtain JdbcTemplate for given credentials.");
        }

        return jdbcTemplate.execute((Connection con) -> {
            List<String> columnList = new ArrayList<>();
            DatabaseMetaData metaData = con.getMetaData();
            try (ResultSet rs = metaData.getColumns(null, "public", tableName, "%")) {
                while (rs.next()) {
                    columnList.add(rs.getString("COLUMN_NAME"));
                }
            }
            return columnList;
        });
    }

    public List<List<Object>> getTableData(DatabaseCredentials credentials, String tableName) throws SQLException {
        String connectionString = dataSourceManager.getConnectionString(credentials);
        JdbcTemplate jdbcTemplate = dataSourceManager.getJdbcTemplateForDb(connectionString);

        if (jdbcTemplate == null) {
            throw new SQLException("Unable to obtain JdbcTemplate for given credentials.");
        }

        return jdbcTemplate.query("SELECT * FROM " + tableName, (rs, rowNum) -> {
            List<Object> row = new ArrayList<>();
            int columnCount = rs.getMetaData().getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                row.add(rs.getObject(i));
            }
            return row;
        });
    }
}
