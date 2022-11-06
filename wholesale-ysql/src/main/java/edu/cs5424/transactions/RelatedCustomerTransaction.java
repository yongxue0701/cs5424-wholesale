package edu.cs5424.transactions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RelatedCustomerTransaction extends BaseTransaction {
    private final int C_W_ID;
    private final int C_D_ID;
    private final int C_ID;
    private Connection conn = null;

    public RelatedCustomerTransaction(final Connection conn, final String[] params) {
        super(conn, params);

        this.conn = conn;
        C_W_ID = Integer.parseInt(params[1]);
        C_D_ID = Integer.parseInt(params[2]);
        C_ID = Integer.parseInt(params[3]);
    }

    @Override
    public void execute() {
        try {
            System.out.println(String.format("------Related Customer: warehouse id: %d, district id: %d, customer id: %d------", this.C_W_ID, this.C_D_ID, this.C_ID));

            Statement stmt = conn.createStatement();

            System.out.println(String.format("This customer: C_W_ID = %s, C_D_ID = %s, C_ID = %s",
                    C_W_ID, C_D_ID, C_ID));
            // find orders for this customer
            ResultSet rs = stmt.executeQuery(String.format("select O_W_ID,O_D_ID,O_ID from orders where O_W_ID = %d and O_D_ID = %d and O_C_ID = %d", C_W_ID, C_D_ID, C_ID));
            ArrayList<HashMap<String, Integer>> orders_this_customer = new ArrayList<HashMap<String, Integer>>();
            while (rs.next()) {
//                System.out.println(String.format("Query returned: O_W_ID = %s, O_D_ID = %s, O_ID = %s",
//                        rs.getString(1), rs.getString(2), rs.getString(3)));
                HashMap<String, Integer> order_this_customer = new HashMap<String, Integer>();
                order_this_customer.put("O_W_ID", rs.getInt(1));
                order_this_customer.put("O_D_ID", rs.getInt(2));
                order_this_customer.put("O_ID", rs.getInt(3));
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
            ResultSet rs_customer = stmt.executeQuery(String.format("select C_W_ID,C_D_ID,C_ID from customer where C_W_ID <> %d", C_W_ID));
            while (rs_customer.next()) {
//                System.out.println(String.format("Query returned: other customer C_W_ID = %s, C_D_ID = %s, C_ID = %s",
//                        rs_customer.getString(1), rs_customer.getString(2), rs_customer.getString(3)));
                HashMap<String, Integer> otherCustomer = new HashMap<String, Integer>();
                otherCustomer.put("C_W_ID", rs_customer.getInt(1));
                otherCustomer.put("C_D_ID", rs_customer.getInt(2));
                otherCustomer.put("C_ID", rs_customer.getInt(3));
                otherCustomers.add(otherCustomer);
            }
            for (HashMap<String, Integer> otherCustomer : otherCustomers) {
                System.out.println(String.format("Other customer C_W_ID = %s, C_D_ID = %s, C_ID = %s",
                        otherCustomer.get("C_W_ID"), otherCustomer.get("C_D_ID"), otherCustomer.get("C_ID")));
                // Find orders for this customer
                ResultSet rs_order = stmt.executeQuery(String.format("select O_W_ID,O_D_ID,O_ID from orders where O_W_ID = %d and O_D_ID = %d and O_C_ID = %d", otherCustomer.get("C_W_ID"), otherCustomer.get("C_D_ID"), otherCustomer.get("C_ID")));
                ArrayList<HashMap<String, Integer>> orders_other_customer = new ArrayList<HashMap<String, Integer>>();
                while (rs_order.next()) {
//                    System.out.println(String.format("Query returned: O_W_ID = %s, O_D_ID = %s, O_ID = %s",
//                            rs_order.getString(1), rs_order.getString(2), rs_order.getString(3)));
                    HashMap<String, Integer> order_other_customer = new HashMap<String, Integer>();
                    order_other_customer.put("O_W_ID", rs_order.getInt(1));
                    order_other_customer.put("O_D_ID", rs_order.getInt(2));
                    order_other_customer.put("O_ID", rs_order.getInt(3));
                    orders_other_customer.add(order_other_customer);
                }

                boolean found = false;
                for (HashMap<String, Integer> order_this_customer : orders_this_customer) {
                    for (HashMap<String, Integer> order_other_customer : orders_other_customer) {
                        System.out.println("--------------------------------------------");
//                        System.out.println(String.format("This order: O_W_ID = %s, O_D_ID = %s, O_ID = %s",
//                                order_this_customer.get("O_W_ID"), order_this_customer.get("O_D_ID"), order_this_customer.get("O_ID")));
//                        System.out.println(String.format("Other order: O_W_ID = %s, O_D_ID = %s, O_ID = %s",
//                                order_other_customer.get("O_W_ID"), order_other_customer.get("O_D_ID"), order_other_customer.get("O_ID")));
//                        System.out.println("--------------------------------------------");
                        int counter = 0; //number of same order item
                        //find order items for orders_this_customer
                        ResultSet rs_order_item_this = stmt.executeQuery(String.format("select OL_I_ID from order_line where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d", order_this_customer.get("O_W_ID"), order_this_customer.get("O_D_ID"), order_this_customer.get("O_ID")));
//                        ArrayList<HashMap<String, Integer>> order_items_this_customer = new ArrayList<HashMap<String, Integer>>();
                        List<Integer> order_items_this_customer = new ArrayList<Integer>();
                        while (rs_order_item_this.next()) {
//                            System.out.println(String.format("Query returned: this OL_I_ID = %s",
//                                    rs_order_item_this.getString(1)));
//                            HashMap<String, Integer> order_item_this_customer = new HashMap<String, Integer>();
                            order_items_this_customer.add(rs_order_item_this.getInt(1));
//                            order_item_this_customer.put("OL_I_ID", rs_order_item_this.getInt(1));
//                            order_items_this_customer.add(order_item_this_customer);
                        }
                        //find order items for order_other_customer
                        ResultSet rs_order_item_other = stmt.executeQuery(String.format("select OL_I_ID from order_line where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d", order_other_customer.get("O_W_ID"), order_other_customer.get("O_D_ID"), order_other_customer.get("O_ID")));
//                        ArrayList<HashMap<String, Integer>> order_items_other_customer = new ArrayList<HashMap<String, Integer>>();
                        List<Integer> order_items_other_customer = new ArrayList<Integer>();
                        while (rs_order_item_other.next()) {
//                            System.out.println(String.format("Query returned: other OL_I_ID = %s",
//                                    rs_order_item_other.getString(1)));
//                            HashMap<String, Integer> order_item_other_customer = new HashMap<String, Integer>();
                            order_items_other_customer.add(rs_order_item_other.getInt(1));
//                            order_item_other_customer.put("OL_I_ID", rs_order_item_other.getInt(1));
//                            order_items_other_customer.add(order_item_other_customer);
                        }
                        for (Integer order_item_this_customer : order_items_this_customer) {
                            if (order_items_other_customer.contains(order_item_this_customer))
                                counter = counter + 1;
                            if (counter >= 2)
                                break;
                        }
//                        for (HashMap<String, Integer> order_item_this_customer : order_items_this_customer){
//                            for (HashMap<String, Integer> order_item_other_customer : order_items_other_customer){
////                                System.out.println("********************************************");
////                                System.out.println(String.format("This order item: OL_I_ID = %s, other order item: OL_I_ID = %s, equal? %s",
////                                        order_item_this_customer.get("OL_I_ID"), order_item_other_customer.get("OL_I_ID"), order_item_this_customer.get("OL_I_ID") == order_item_other_customer.get("OL_I_ID")));
////                                System.out.println("********************************************");
//                                if(order_item_this_customer.get("OL_I_ID") == order_item_other_customer.get("OL_I_ID")){
//                                    counter = counter + 1;
//                                    break;
//                                }
//                            }
//                            if(counter >= 2){
//                                break;
//                            }
//                        }
                        System.out.println("counter: " + counter);
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
            System.out.println("-----------------------");
        } catch (SQLException e) {//SQLException
            System.err.println(e.getMessage());
        }
    }
}
