package edu.cs5424.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.ResultSet;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class RelatedCustomerTransaction {
    //
//  public static final String SELECT_ORDERS = "SELECT O_L_INFO FROM customer_order WHERE O_W_ID = %d AND O_D_ID = %d AND O_C_ID = %d ALLOW FILTERING";
//
//  public static final String SELECT_ITEMS = "SELECT I_O_ID_LIST FROM item WHERE I_ID IN (%s)";
//
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

//    System.out.println(String.format("This customer: C_W_ID = %s, C_D_ID = %s, C_ID = %s",
//            C_W_ID, C_D_ID, C_ID));
        // find orders for this customer
        ResultSet rs = session.execute(String.format("select O_W_ID,O_D_ID,O_ID from wholesale.orders where O_W_ID = %d and O_D_ID = %d and O_C_ID = %d", C_W_ID, C_D_ID, C_ID));
        ArrayList<HashMap<String, Integer>> orders_this_customer = new ArrayList<HashMap<String, Integer>>();
        for (Row row : rs.all()) {
//      System.out.println(String.format("Query returned: O_W_ID = %s, O_D_ID = %s, O_ID = %s",
//              row.getInt("O_W_ID"), row.getInt("O_D_ID"), row.getInt("O_ID")));
            HashMap<String, Integer> order_this_customer = new HashMap<String, Integer>();
            order_this_customer.put("O_W_ID", row.getInt("O_W_ID"));
            order_this_customer.put("O_D_ID", row.getInt("O_D_ID"));
            order_this_customer.put("O_ID", row.getInt("O_ID"));
            orders_this_customer.add(order_this_customer);
        }

        ArrayList<HashMap<String, Integer>> relatedCustomers = new ArrayList<HashMap<String, Integer>>();
        // Add input customer first
        HashMap<String, Integer> thisCustomer = new HashMap<String, Integer>();
        thisCustomer.put("C_W_ID", C_W_ID);
        thisCustomer.put("C_D_ID", C_D_ID);
        thisCustomer.put("C_ID", C_ID);
        relatedCustomers.add(thisCustomer);

        // Find related customers
        ArrayList<HashMap<String, Integer>> otherCustomers = new ArrayList<HashMap<String, Integer>>();
        ResultSet rs_customer = session.execute(String.format("select C_W_ID,C_D_ID,C_ID from wholesale.customer where C_W_ID <> %d", C_W_ID));
        for (Row row : rs_customer.all()) {
//      System.out.println(String.format("Query returned: other customer C_W_ID = %s, C_D_ID = %s, C_ID = %s",
//              row.getInt("C_W_ID"), row.getInt("C_D_ID"), row.getInt("C_ID")));
            HashMap<String, Integer> otherCustomer = new HashMap<String, Integer>();
            otherCustomer.put("C_W_ID", row.getInt("C_W_ID"));
            otherCustomer.put("C_D_ID", row.getInt("C_D_ID"));
            otherCustomer.put("C_ID", row.getInt("C_ID"));
            otherCustomers.add(otherCustomer);
        }
        for (HashMap<String, Integer> otherCustomer : otherCustomers) {
//            System.out.println(String.format("Other customer C_W_ID = %s, C_D_ID = %s, C_ID = %s",
//                    otherCustomer.get("C_W_ID"), otherCustomer.get("C_D_ID"), otherCustomer.get("C_ID")));
            // Find orders for this customer
            ResultSet rs_order = session.execute(String.format("select O_W_ID,O_D_ID,O_ID from wholesale.orders where O_W_ID = %d and O_D_ID = %d and O_C_ID = %d", otherCustomer.get("C_W_ID"), otherCustomer.get("C_D_ID"), otherCustomer.get("C_ID")));
            ArrayList<HashMap<String, Integer>> orders_other_customer = new ArrayList<HashMap<String, Integer>>();
            for (Row row : rs_order.all()) {
//        System.out.println(String.format("Query returned: O_W_ID = %s, O_D_ID = %s, O_ID = %s",
//                row.getInt("O_W_ID"), row.getInt("O_D_ID"), row.getInt("O_ID")));
                HashMap<String, Integer> order_other_customer = new HashMap<String, Integer>();
                order_other_customer.put("O_W_ID", row.getInt("O_W_ID"));
                order_other_customer.put("O_D_ID", row.getInt("O_D_ID"));
                order_other_customer.put("O_ID", row.getInt("O_ID"));
                orders_other_customer.add(order_other_customer);
            }

            boolean found = false;
            for (HashMap<String, Integer> order_this_customer : orders_this_customer) {
                for (HashMap<String, Integer> order_other_customer : orders_other_customer) {
//          System.out.println("--------------------------------------------");
//          System.out.println(String.format("This order: O_W_ID = %s, O_D_ID = %s, O_ID = %s",
//                                order_this_customer.get("O_W_ID"), order_this_customer.get("O_D_ID"), order_this_customer.get("O_ID")));
//          System.out.println(String.format("Other order: O_W_ID = %s, O_D_ID = %s, O_ID = %s",
//                                order_other_customer.get("O_W_ID"), order_other_customer.get("O_D_ID"), order_other_customer.get("O_ID")));
//          System.out.println("--------------------------------------------");
                    int counter = 0; //number of same order item
                    //find order items for orders_this_customer
                    ResultSet rs_order_item_this = session.execute(String.format("select OL_I_ID from wholesale.order_line where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d", order_this_customer.get("O_W_ID"), order_this_customer.get("O_D_ID"), order_this_customer.get("O_ID")));
//                        ArrayList<HashMap<String, Integer>> order_items_this_customer = new ArrayList<HashMap<String, Integer>>();
                    List<Integer> order_items_this_customer = new ArrayList<Integer>();
                    for (Row row : rs_order_item_this) {
//            System.out.println(String.format("Query returned: this OL_I_ID = %s",
//                    row.getInt("OL_I_ID")));
                        order_items_this_customer.add(row.getInt("OL_I_ID"));
                    }
                    //find order items for order_other_customer
                    ResultSet rs_order_item_other = session.execute(String.format("select OL_I_ID from wholesale.order_line where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d", order_other_customer.get("O_W_ID"), order_other_customer.get("O_D_ID"), order_other_customer.get("O_ID")));
                    List<Integer> order_items_other_customer = new ArrayList<Integer>();
                    for (Row row : rs_order_item_other) {
//            System.out.println(String.format("Query returned: other OL_I_ID = %s",
//                    row.getInt("OL_I_ID")));
                        order_items_other_customer.add(row.getInt("OL_I_ID"));
                    }
                    for (Integer order_item_this_customer : order_items_this_customer) {
                        if (order_items_other_customer.contains(order_item_this_customer))
                            counter = counter + 1;
                        if (counter >= 2)
                            break;
                    }
//          System.out.println("counter: " + counter);
                    if (counter >= 2) {
                        relatedCustomers.add(otherCustomer);
                        found = true;
                        break;
                    }
                }
                if (found == true)
                    break;
            }
//                break; // test one other customer only
        }
        System.out.println("relatedCustomers: " + relatedCustomers);

    }
}
