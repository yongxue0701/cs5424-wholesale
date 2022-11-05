package com.yugabyte.transactions;

import java.sql.*;
import java.util.*;

public class StockLevelTransaction {
    private final String QUERY_GET_NEXT_ORDER_ID = "SELECT d_next_o_id FROM district " +
            "WHERE d_w_id = %d " +
            "AND d_id = %d;";
    private final String QUERY_GET_LAST_L_ORDERS = "SELECT o_id FROM orders " +
            "WHERE o_w_id = %d " +
            "AND o_d_id = %d " +
            "AND o_id >= %d " +
            "AND o_id < %d;";
    private final String QUERY_GET_ITEM_IDS = "SELECT ol_i_id FROM order_line " +
            "WHERE ol_w_id = %d " +
            "AND ol_d_id = %d " +
            "AND ol_o_id = %d;";
    private final String QUERY_GET_STOCK_QTY = "SELECT s_quantity FROM stock " +
            "WHERE s_w_id = %d " +
            "AND s_i_id = %d;";

    public void getStockLevel(Connection conn, int warehouseID, int districtID, int stockThreshold, int numOfLastOrders) {
        try {
            System.out.println("------Stock Level------");

            Set<Integer> itemIDs = new HashSet<>();
            int nextOrderID = -1;
            int count = 0;

            PreparedStatement getNextOrderIDStmt = conn.prepareStatement(String.format(QUERY_GET_NEXT_ORDER_ID, warehouseID, districtID));
            ResultSet nextOrder = getNextOrderIDStmt.executeQuery();
            while (nextOrder.next()) {
                nextOrderID = nextOrder.getInt("d_next_o_id");
            }

            if (nextOrderID == -1) {
                System.out.printf("Invalid Next Order ID, Warehouse ID: %d, District ID: %d\n", warehouseID, districtID);
            }

            PreparedStatement getLastLOrdersStmt = conn.prepareStatement(String.format(QUERY_GET_LAST_L_ORDERS, warehouseID, districtID, nextOrderID - numOfLastOrders, nextOrderID));
            ResultSet lastOrders = getLastLOrdersStmt.executeQuery();
            while (lastOrders.next()) {
                PreparedStatement getItemIDsStmt = conn.prepareStatement(String.format(QUERY_GET_ITEM_IDS, warehouseID, districtID, lastOrders.getInt("o_id")));
                ResultSet items = getItemIDsStmt.executeQuery();

                while (items.next()) {
                    itemIDs.add(items.getInt("ol_i_id"));
                }
            }

            for (int itemID : itemIDs) {
                PreparedStatement getStockQtyStmt = conn.prepareStatement(String.format(QUERY_GET_STOCK_QTY, warehouseID, itemID));
                ResultSet stockQty = getStockQtyStmt.executeQuery();

                while (stockQty.next()) {
                    if (stockQty.getBigDecimal("s_quantity").intValue() < stockThreshold) {
                        count++;
                    }
                }
            }

            System.out.printf("Total number of items in S where its stock quantity is below the threshold: %d\n", count);
            System.out.println("-----------------------");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
