package edu.cs5424;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import edu.cs5424.transactions.OrderStatusTransaction;
import edu.cs5424.transactions.StockLevelTransaction;

public class Main {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/postgres",
                "postgres",
                "root");

        Statement stmt = conn.createStatement();

        OrderStatusTransaction orderStatusTxn = new OrderStatusTransaction();
        orderStatusTxn.getOrderStatus(conn, 1, 1, 2056);

        StockLevelTransaction stockLevelTxn = new StockLevelTransaction();
        stockLevelTxn.getStockLevel(conn, 1, 1, 14, 27);

        System.out.println("Connected to the YugabyteDB Cluster successfully.");
    }
}