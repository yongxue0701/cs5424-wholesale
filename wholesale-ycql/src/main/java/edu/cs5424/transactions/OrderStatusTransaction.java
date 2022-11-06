package edu.cs5424.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

public class OrderStatusTransaction extends BaseTransaction {
    private final String QUERY_GET_CUSTOMER_DETAILS = "SELECT c_first, c_middle, c_last, c_balance FROM wholesale.customer " +
            "WHERE c_w_id = %d " +
            "AND c_d_id = %d " +
            "AND c_id = %d;";
    private final String QUERY_GET_LAST_ORDER = "SELECT o_id, o_entry_d, o_carrier_id FROM wholesale.orders_by_timestamp " +
            "WHERE o_w_id = %d " +
            "AND o_d_id = %d " +
            "AND o_c_id = %d " +
            "LIMIT 1 ALLOW FILTERING;";
    private final String QUERY_GET_ITEM_DETAILS = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM wholesale.order_line " +
            "WHERE ol_w_id = %d " +
            "AND ol_d_id = %d " +
            "AND ol_o_id = %d;";
    private final int warehouseID;
    private final int districtID;
    private final int customerID;
    private CqlSession session = null;

    public OrderStatusTransaction(final CqlSession session, final String[] params) {
        super(session, params);

        this.session = session;
        this.warehouseID = Integer.parseInt(params[1]);
        this.districtID = Integer.parseInt(params[2]);
        this.customerID = Integer.parseInt(params[3]);
    }

    @Override
    public void execute() {
        try {
            System.out.println(String.format("------Order Status: warehouse id: %d, district id: %d, customer id: %d------", this.warehouseID, this.districtID, this.customerID));

            int orderID = -1;

            System.out.println("---Customer Details: ");
            ResultSet customer = this.session.execute(String.format(QUERY_GET_CUSTOMER_DETAILS, this.warehouseID, this.districtID, this.customerID));
            for (Row row : customer.all()) {
                System.out.printf("First Name: %s, Middle Name: %s, Last Name: %s, Balance: %d\n",
                        row.getString("c_first"), row.getString("c_middle"),
                        row.getString("c_last"), row.getBigDecimal("c_balance").intValue());
            }

            System.out.println("---Last Order Details: ");
            ResultSet lastOrder = this.session.execute(String.format(QUERY_GET_LAST_ORDER, this.warehouseID, this.districtID, this.customerID));
            for (Row row : lastOrder.all()) {
                orderID = row.getInt("o_id");
                System.out.printf("Order Number: %d, Entry Time: %s, Carrier Identifier: %d\n",
                        row.getInt("o_id"), row.getInstant("o_entry_d").toString(), row.getInt("o_carrier_id"));
            }

            if (orderID == -1) {
                System.out.printf("Invalid Order Number, Customer ID: %d, Warehouse ID: %d, District ID: %d\n",
                        this.customerID, this.warehouseID, this.districtID);
            }

            System.out.println("---Item Details: ");
            ResultSet itemDetails = this.session.execute(String.format(QUERY_GET_ITEM_DETAILS, this.warehouseID, this.districtID, orderID));
            for (Row row : itemDetails.all()) {
                String deliveryTime = "NULL";
                if (row.getLocalTime("ol_delivery_d") != null) {
                    deliveryTime = row.getInstant("ol_delivery_d").toString();
                }

                System.out.printf("Item Number: %d, Supplying Warehouse Number: %d, Quantity: %d, Amount: %d, Delivery Time: %s\n",
                        row.getInt("ol_i_id"), row.getInt("ol_supply_w_id"),
                        row.getBigDecimal("ol_quantity").intValue(), row.getBigDecimal("ol_amount").intValue(),
                        deliveryTime);
            }

            System.out.println("-----------------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
