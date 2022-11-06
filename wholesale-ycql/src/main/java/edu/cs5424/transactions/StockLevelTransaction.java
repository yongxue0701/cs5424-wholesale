package edu.cs5424.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import java.util.*;

public class StockLevelTransaction extends BaseTransaction {
    private final String QUERY_GET_NEXT_ORDER_ID = "SELECT d_next_o_id FROM wholesale.district " +
            "WHERE d_w_id = %d " +
            "AND d_id = %d;";
    private final String QUERY_GET_LAST_L_ORDERS = "SELECT o_id FROM wholesale.orders " +
            "WHERE o_w_id = %d " +
            "AND o_d_id = %d " +
            "AND o_id IN (%s);";
    private final String QUERY_GET_ITEM_IDS = "SELECT ol_i_id FROM wholesale.order_line " +
            "WHERE ol_w_id = %d " +
            "AND ol_d_id = %d " +
            "AND ol_o_id = %d;";
    private final String QUERY_GET_STOCK_QTY = "SELECT s_quantity FROM wholesale.stock " +
            "WHERE s_w_id = %d " +
            "AND s_i_id = %d;";
    private final int warehouseID;
    private final int districtID;
    private final int stockThreshold;
    private final int numOfLastOrders;
    private CqlSession session = null;

    public StockLevelTransaction(final CqlSession session, final String[] params) {
        super(session, params);

        this.session = session;
        this.warehouseID = Integer.parseInt(params[1]);
        this.districtID = Integer.parseInt(params[2]);
        this.stockThreshold = Integer.parseInt(params[3]);
        this.numOfLastOrders = Integer.parseInt(params[4]);
    }

    @Override
    public void execute() {
        try {
            System.out.println(String.format("------Stock Level: warehouse id: %d, district id: %d, stock threshold: %d, last order: %d------", this.warehouseID, this.districtID, this.stockThreshold, this.numOfLastOrders));

            Set<Integer> itemIDs = new HashSet<>();
            int nextOrderID = -1;
            int count = 0;

            ResultSet nextOrder = this.session.execute(String.format(QUERY_GET_NEXT_ORDER_ID, this.warehouseID, this.districtID));
            for (Row row : nextOrder.all()) {
                nextOrderID = row.getInt("d_next_o_id");
            }

            if (nextOrderID == -1) {
                System.out.printf("Invalid Next Order ID, Warehouse ID: %d, District ID: %d\n", this.warehouseID, this.districtID);
            }

            List<String> orderIDs = new ArrayList<>();
            for (int i = nextOrderID - this.numOfLastOrders; i < nextOrderID; i++) {
                Integer orderID = new Integer(i);
                orderIDs.add(orderID.toString());
            }
            String orderIDRange = String.join(",", orderIDs);

            ResultSet lastOrders = this.session.execute(String.format(QUERY_GET_LAST_L_ORDERS, this.warehouseID, this.districtID, orderIDRange));
            for (Row row : lastOrders.all()) {
                ResultSet items = this.session.execute(String.format(QUERY_GET_ITEM_IDS, this.warehouseID, this.districtID, row.getInt("o_id")));

                for (Row row2 : items.all()) {
                    itemIDs.add(row2.getInt("ol_i_id"));
                }
            }

            for (int itemID : itemIDs) {
                ResultSet stockQty = this.session.execute(String.format(QUERY_GET_STOCK_QTY, this.warehouseID, itemID));

                for (Row row2 : stockQty.all()) {
                    if (row2.getBigDecimal("s_quantity").intValue() < this.stockThreshold) {
                        count++;
                    }
                }
            }

            System.out.printf("Total number of items in S where its stock quantity is below the threshold: %d\n", count);
            System.out.println("-----------------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}