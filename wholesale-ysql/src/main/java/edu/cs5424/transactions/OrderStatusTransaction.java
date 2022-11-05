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
    private final int warehouseID;
    private final int districtID;
    private final int customerID;
    private Connection conn = null;

    public OrderStatusTransaction(final Connection connection, final String[] parameters) {
        this.conn = connection;
        this.warehouseID = Integer.parseInt(parameters[1]);
        this.districtID = Integer.parseInt(parameters[2]);
        this.customerID = Integer.parseInt(parameters[3]);
    }

    public void execute() {
        try {
            System.out.println(String.format("------Order Status: warehouse id: %d, district id: %d, customer id: %d------", this.warehouseID, this.districtID, this.customerID));

            int orderID = -1;

            System.out.println("---Customer Details: ");
            PreparedStatement getCustomerDetailsStmt = this.conn.prepareStatement(String.format(QUERY_GET_CUSTOMER_DETAILS, this.warehouseID, this.districtID, this.customerID));
            ResultSet customer = getCustomerDetailsStmt.executeQuery();
            while (customer.next()) {
                System.out.printf("First Name: %s, Middle Name: %s, Last Name: %s, Balance: %d\n",
                        customer.getString("c_first"), customer.getString("c_middle"),
                        customer.getString("c_last"), customer.getBigDecimal("c_balance").intValue());
            }

            System.out.println("---Last Order Details: ");
            PreparedStatement getLastOrderStmt = this.conn.prepareStatement(String.format(QUERY_GET_LAST_ORDER, this.warehouseID, this.districtID, this.customerID));
            ResultSet lastOrder = getLastOrderStmt.executeQuery();
            while (lastOrder.next()) {
                orderID = lastOrder.getInt("o_id");
                System.out.printf("Order Number: %d, Entry Time: %s, Carrier Identifier: %d\n",
                        lastOrder.getInt("o_id"), lastOrder.getTimestamp("o_entry_d").toString(),
                        lastOrder.getInt("o_carrier_id"));
            }

            if (orderID == -1) {
                System.out.printf("Invalid Order Number, Customer ID: %d, Warehouse ID: %d, District ID: %d\n", this.customerID, this.warehouseID, this.districtID);
            }

            System.out.println("---Item Details: ");
            PreparedStatement getItemDetailsStmt = this.conn.prepareStatement(String.format(QUERY_GET_ITEM_DETAILS, this.warehouseID, this.districtID, orderID));
            ResultSet itemDetails = getItemDetailsStmt.executeQuery();
            while (itemDetails.next()) {
                String deliveryTime = "NULL";
                if (itemDetails.getTimestamp("ol_delivery_d") != null) {
                    deliveryTime = itemDetails.getTimestamp("ol_delivery_d").toString();
                }

                System.out.printf("Item Number: %d, Supplying Warehouse Number: %d, Quantity: %d, Amount: %d, Delivery Time: %s\n",
                        itemDetails.getInt("ol_i_id"), itemDetails.getInt("ol_supply_w_id"),
                        itemDetails.getBigDecimal("ol_quantity").intValue(), itemDetails.getBigDecimal("ol_amount").intValue(),
                        deliveryTime);
            }

            System.out.println("-----------------------");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
