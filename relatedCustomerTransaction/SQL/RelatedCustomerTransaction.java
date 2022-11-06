package com.yugabyte.transactions;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;

public class RelatedCustomerTransaction {
    private final int C_W_ID;
    private final int C_D_ID;
    private final int C_ID;
    private Connection conn = null;

    public RelatedCustomerTransaction(final Connection connection, final String[] parameters) {
        conn = connection;
        C_W_ID = Integer.parseInt(parameters[1]);
        C_D_ID = Integer.parseInt(parameters[2]);
        C_ID = Integer.parseInt(parameters[3]);
    }

    public void execute(){
        try {
            ArrayList<ArrayList<Integer>> relatedCustomers=new ArrayList<ArrayList<Integer>>();
            // Add input customer first
            ArrayList<Integer> thisCustomer=new ArrayList<Integer>();
            thisCustomer.add(C_W_ID);
            thisCustomer.add(C_D_ID);
            thisCustomer.add(C_ID);
            relatedCustomers.add(thisCustomer);

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(
                    "select ol_other.C_W_ID, ol_other.C_D_ID, ol_other.C_ID, count(*) from (\n" +
                            "select C_W_ID, C_D_ID, C_ID, O_W_ID, O_D_ID, O_C_ID, O_ID, OL_W_ID, OL_O_ID, OL_D_ID, OL_I_ID from (\n" +
                            "select C_W_ID, C_D_ID, C_ID\n" +
                            "from customer\n" +
                            "where C_W_ID<>%d\n" +
                            ") as c_other\n" +
                            "left join orders on c_other.C_W_ID=orders.O_W_ID and c_other.C_D_ID=orders.O_D_ID and c_other.C_ID=orders.O_C_ID\n" +
                            "left join order_line on order_line.OL_W_ID=orders.O_W_ID and order_line.OL_D_ID=orders.O_D_ID and order_line.OL_O_ID=orders.O_ID\n"+
                    ") as ol_other\n" +
                    "inner join (\n" +
                            "select C_W_ID, C_D_ID, C_ID, O_W_ID, O_D_ID, O_C_ID, O_ID, OL_W_ID, OL_O_ID, OL_D_ID, OL_I_ID from (\n" +
                            "select C_W_ID, C_D_ID, C_ID\n" +
                            "from customer\n" +
                            "where C_W_ID=%d and C_D_ID=%d and C_ID=%d\n" +
                            ") as c_this\n" +
                            "left join orders on c_this.C_W_ID=orders.O_W_ID and c_this.C_D_ID=orders.O_D_ID and c_this.C_ID=orders.O_C_ID\n" +
                            "left join order_line on order_line.OL_W_ID=orders.O_W_ID and order_line.OL_D_ID=orders.O_D_ID and order_line.OL_O_ID=orders.O_ID\n"+
                    ") as ol_this\n" +
                    "on ol_other.OL_I_ID=ol_this.OL_I_ID\n" +
                    "group by ol_other.C_W_ID, ol_other.C_D_ID, ol_other.C_ID, ol_other.O_ID\n" +
                    "having count(*)>1", C_W_ID, C_W_ID, C_D_ID, C_ID));

            while(rs.next()){
                ArrayList<Integer> otherCustomer=new ArrayList<Integer>();
                otherCustomer.add(rs.getInt(1));
                otherCustomer.add(rs.getInt(2));
                otherCustomer.add(rs.getInt(3));
                relatedCustomers.add(otherCustomer);
            }
            System.out.println("relatedCustomers: "+relatedCustomers);
        }catch (SQLException  e) {//SQLException
            System.err.println(e.getMessage());
        }
    }
}
