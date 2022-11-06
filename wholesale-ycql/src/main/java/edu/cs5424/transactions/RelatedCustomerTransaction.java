package edu.cs5424.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.ResultSet;

import java.util.ArrayList;

public class RelatedCustomerTransaction {
    private final int C_W_ID;
    private final int C_D_ID;
    private final int C_ID;
    CqlSession session;

    public RelatedCustomerTransaction(CqlSession sess, final String[] parameters) {
        session = sess;
        C_W_ID = Integer.parseInt(parameters[1]);
        C_D_ID = Integer.parseInt(parameters[2]);
        C_ID = Integer.parseInt(parameters[3]);
    }

    public void execute() {
        try {
            ArrayList<ArrayList<Integer>> relatedCustomers = new ArrayList<ArrayList<Integer>>();
            // Add input customer first
            ArrayList<Integer> thisCustomer = new ArrayList<Integer>();
            thisCustomer.add(C_W_ID);
            thisCustomer.add(C_D_ID);
            thisCustomer.add(C_ID);
            relatedCustomers.add(thisCustomer);
            ResultSet rs = session.execute(String.format("select O_W_ID,O_D_ID,O_ID from wholesale.orders where O_W_ID = %d and O_D_ID = %d and O_C_ID = %d", C_W_ID, C_D_ID, C_ID));
            ArrayList<Integer> order_items_this_customer = new ArrayList<Integer>();
            for (Row row : rs.all()) {
                ResultSet rs_order_item_this = session.execute(String.format("select OL_I_ID from wholesale.order_line where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d", row.getInt("O_W_ID"), row.getInt("O_D_ID"), row.getInt("O_ID")));
                for (Row row1 : rs_order_item_this) {
                    order_items_this_customer.add(row1.getInt("OL_I_ID"));
                }
            }
            ArrayList<Row> order_items = new ArrayList<Row>();
            for (Integer i = 0; i < order_items_this_customer.size(); i++) {
                String ORDER_LINE_QUERY = "select OL_W_ID, OL_D_ID, OL_O_ID, OL_I_ID from wholesale.order_line where OL_W_ID<>" + C_W_ID;
                ORDER_LINE_QUERY = ORDER_LINE_QUERY + " and ";
                ORDER_LINE_QUERY = ORDER_LINE_QUERY + "OL_I_ID=" + order_items_this_customer.get(i);
                ResultSet rs_order_item = session.execute(ORDER_LINE_QUERY);
                order_items.addAll(rs_order_item.all());
            }

            ArrayList<Row> orders = new ArrayList<Row>();
            for (Row item : order_items) {
                String ORDER_QUERY = String.format("select O_W_ID, O_D_ID, O_C_ID, O_ID from wholesale.orders where O_W_ID=%d and O_D_ID=%d and O_ID=%d", item.getInt("OL_W_ID"), item.getInt("OL_D_ID"), item.getInt("OL_O_ID"));
                ResultSet rs_order = session.execute(ORDER_QUERY);
                orders.addAll(rs_order.all());
            }
            ArrayList<ArrayList<Integer>> orders_check = new ArrayList<ArrayList<Integer>>();
            for (Row row : orders) {
                ArrayList<Integer> order_check = new ArrayList<Integer>();
                order_check.add(row.getInt("O_W_ID"));
                order_check.add(row.getInt("O_D_ID"));
                order_check.add(row.getInt("O_C_ID"));
                order_check.add(row.getInt("O_ID"));
                if (orders_check.contains(order_check)) {
                    order_check.remove(order_check.size() - 1);
                    relatedCustomers.add(order_check);
                } else {
                    orders_check.add(order_check);
                }
            }
            System.out.println("relatedCustomers: " + relatedCustomers);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
