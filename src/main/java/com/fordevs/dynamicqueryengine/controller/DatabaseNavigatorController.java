package com.fordevs.dynamicqueryengine.controller;

import com.fordevs.dynamicqueryengine.dto.DatabaseCredentials;
import com.fordevs.dynamicqueryengine.service.DatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@RestController
@RequestMapping("/api/database")
public class DatabaseNavigatorController {

    @Autowired
    private DatabaseService databaseService;

    // Endpoint to connect to a database
    @PostMapping("/connect")
    public ResponseEntity<String> connectToDatabase(@RequestBody DatabaseCredentials credentials) {
        try {
            databaseService.connect(credentials);
            return ResponseEntity.ok("Connected to the database successfully.");
        } catch (SQLException e) {
            return ResponseEntity.status(500).body("Failed to connect to the database: " + e.getMessage());
        }
    }

    // Endpoint to list all tables
    @GetMapping("/tables")
    public ResponseEntity<List<String>> listTables(@RequestBody DatabaseCredentials credentials) {
        try (Connection connection = databaseService.connect(credentials)) {
            List<String> tables = databaseService.listTables(connection);
            return ResponseEntity.ok(tables);
        } catch (SQLException e) {
            return ResponseEntity.status(500).body(null);
        }
    }

    // Endpoint to list columns in a table
    @GetMapping("/columns")
    public ResponseEntity<List<String>> listColumns(@RequestBody DatabaseCredentials credentials, @RequestParam String tableName) {
        try (Connection connection = databaseService.connect(credentials)) {
            List<String> columns = databaseService.listColumns(connection, tableName);
            return ResponseEntity.ok(columns);
        } catch (SQLException e) {
            return ResponseEntity.status(500).body(null);
        }
    }

    // Endpoint to execute a query
    @PostMapping("/query")
    public ResponseEntity<?> executeQuery(@RequestBody DatabaseCredentials credentials, @RequestParam String query) {
        try (Connection connection = databaseService.connect(credentials)) {
            ResultSet resultSet = databaseService.executeQuery(connection, query);
            // Assuming that you have some mechanism to handle the ResultSet (like streaming the results back or processing)
            // You would handle the ResultSet and return an appropriate ResponseEntity.
            // This part of the implementation depends on your existing setup.
            return ResponseEntity.ok("Query executed successfully.");
        } catch (SQLException e) {
            return ResponseEntity.status(500).body("Failed to execute query: " + e.getMessage());
        }
    }

    // Endpoint to close a database connection
    @PostMapping("/close")
    public ResponseEntity<String> closeConnection(@RequestBody DatabaseCredentials credentials) {
        try (Connection connection = databaseService.connect(credentials)) {
            databaseService.closeConnection(connection);
            return ResponseEntity.ok("Connection closed successfully.");
        } catch (SQLException e) {
            return ResponseEntity.status(500).body("Failed to close the connection: " + e.getMessage());
        }
    }
}
