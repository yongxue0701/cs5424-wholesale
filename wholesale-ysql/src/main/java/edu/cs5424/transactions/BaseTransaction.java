package edu.cs5424.transactions;

import java.sql.*;

public abstract class BaseTransaction {
    private final Connection conn;
    private final String[] params;

    public BaseTransaction(final Connection conn, final String[] params) {
        this.conn = conn;
        this.params = params;
    }

    public abstract void execute() throws SQLException;

    public ResultSet executeSQLQuery(String query) throws SQLException {
        Statement stmt = conn.createStatement();
        return stmt.executeQuery(query);
    }

    public void executeSQL(String query) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(query);
    }
}