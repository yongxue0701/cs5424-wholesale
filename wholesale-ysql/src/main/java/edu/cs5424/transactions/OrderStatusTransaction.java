package edu.cs5424.transactions;

import java.sql.*;

public class OrderStatusTransaction {
    private final String QUERY_GET_CUSTOMER_DETAILS = "SELECT c_first, c_middle, c_last, c_balance FROM customer " +
            "WHERE c_w_id = %d " +
            "AND c_d_id = %d " +
            "AND c_id = %d;";
    private final String QUERY_GET_LAST_ORDER = "SELECT o_id, o_entry_d, o_carrier_id FROM orders " +
            "WHERE o_w_id = %d " +
            "AND o_d_id = %d " +
            "AND o_c_id = %d " +
            "ORDER BY o_id DESC LIMIT 1;";
    private final String QUERY_GET_ITEM_DETAILS = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line " +
            "WHERE ol_w_id = %d " +
            "AND ol_d_id = %d " +
            "AND ol_o_id = %d;";

    public void getOrderStatus(Connection conn, int warehouseID, int districtID, int customerID) {
        try {
            System.out.println("------Order Status------");

            int orderID = -1;

            System.out.println("Customer Details: ");
            PreparedStatement getCustomerDetailsStmt = conn.prepareStatement(String.format(QUERY_GET_CUSTOMER_DETAILS, warehouseID, districtID, customerID));
            ResultSet customer = getCustomerDetailsStmt.executeQuery();
            while (customer.next()) {
                System.out.printf("First Name: %s, Middle Name: %s, Last Name: %s, Balance: %d\n",
                        customer.getString("c_first"), customer.getString("c_middle"),
                        customer.getString("c_last"), customer.getBigDecimal("c_balance").intValue());
            }

            System.out.println("Last Order Details: ");
            PreparedStatement getLastOrderStmt = conn.prepareStatement(String.format(QUERY_GET_LAST_ORDER, warehouseID, districtID, customerID));
            ResultSet lastOrder = getLastOrderStmt.executeQuery();
            while (lastOrder.next()) {
                orderID = lastOrder.getInt("o_id");
                System.out.printf("Order Number: %d, Entry Time: %s, Carrier Identifier: %d\n",
                        lastOrder.getInt("o_id"), lastOrder.getTimestamp("o_entry_d").toString(),
                        lastOrder.getInt("o_carrier_id"));
            }

            if (orderID == -1) {
                System.out.printf("Invalid Order Number, Customer ID: %d, Warehouse ID: %d, District ID: %d\n", customerID, warehouseID, districtID);
            }

            System.out.println("Item Details: ");
            PreparedStatement getItemDetailsStmt = conn.prepareStatement(String.format(QUERY_GET_ITEM_DETAILS, warehouseID, districtID, orderID));
            ResultSet itemDetails = getItemDetailsStmt.executeQuery();
            while (itemDetails.next()) {
                System.out.printf("Item Number: %d, Supplying Warehouse Number: %d, Quantity: %d, Amount: %d, Delivery Time: %s\n",
                        itemDetails.getInt("ol_i_id"), itemDetails.getInt("ol_supply_w_id"),
                        itemDetails.getBigDecimal("ol_quantity").intValue(), itemDetails.getBigDecimal("ol_amount").intValue(),
                        itemDetails.getTimestamp("ol_delivery_d").toString());
            }

            System.out.println("-----------------------");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
