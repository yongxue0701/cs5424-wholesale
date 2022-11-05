package edu.cs5424.transactions;

import java.math.BigDecimal;
import java.sql.*;
import java.text.MessageFormat;
import java.util.HashMap;

public class PopularItemTransaction {
    private PreparedStatement data_statement;
    private PreparedStatement percentage_statement;

    public PopularItemTransaction(final Connection connection, final String[] parameters) {
        String w_id = parameters[1];
        String d_id = parameters[2];
        // number of last orders
        String l = parameters[3];

        try {
            data_statement = connection.prepareStatement(
                MessageFormat.format(
                "select order_line.ol_d_id, order_line.ol_w_id, o_id, o_entry_d, c_first, c_middle, c_last, i_name, ol_quantity\n" +
                        "from order_line\n" +
                        "join (\n" +
                        "    select t.ol_o_id, t.ol_d_id, t.ol_w_id, max(ol_quantity) as max_ol_quantity\n" +
                        "    from order_line as t\n" +
                        "    where t.ol_o_id in ( -- collection S\n" +
                        "        select o_id from orders\n" +
                        "        join district d on orders.o_d_id = d.d_id\n" +
                        "        where d.d_id =  {0} \n" +
                        "            and d.d_w_id = {1}\n" +
                        "            and orders.o_d_id = {0}\n" +
                        "            and orders.o_w_id = {1}\n" +
                        "            and orders.o_id >= d_next_o_id - {2}\n" +
                        "            and orders.o_id < d_next_o_id\n" +
                        "    )\n" +
                        "    and t.ol_d_id = {0}\n" +
                        "    and t.ol_w_id= {1}\n" +
                        "    group by t.ol_o_id, t.ol_d_id, t.ol_w_id\n" +
                        ") as Ix\n" +
                        "on order_line.ol_o_id = Ix.ol_o_id\n" +
                        "    and order_line.ol_d_id = Ix.ol_d_id\n" +
                        "    and order_line.ol_w_id = Ix.ol_w_id\n" +
                        "    and order_line.ol_quantity = Ix.max_ol_quantity\n" +
                        "join orders o\n" +
                        "    on order_line.ol_w_id = o.o_w_id and order_line.ol_d_id = o.o_d_id and order_line.ol_o_id = o.o_id\n" +
                        "join customer c\n" +
                        "    on o.o_w_id = c.c_w_id and o.o_d_id = c.c_d_id and o.o_c_id = c.c_id\n" +
                        "join item on order_line.ol_i_id = item.i_id;",
                        d_id, w_id, l
                )
            );

            percentage_statement = connection.prepareStatement(
                MessageFormat.format(
            "select i_name, cast(count(order_line.ol_o_id) as decimal) / {2} as percent\n" +
                    "from order_line\n" +
                    "join (\n" +
                    "    select t.ol_o_id, t.ol_d_id, t.ol_w_id, max(ol_quantity) as max_ol_quantity\n" +
                    "    from order_line as t\n" +
                    "    where t.ol_o_id in ( -- collection S\n" +
                    "        select o_id from orders\n" +
                    "        join district d on orders.o_d_id = d.d_id\n" +
                    "        where d.d_id =  {0}\n" +
                    "            and d.d_w_id = {1}\n" +
                    "            and orders.o_d_id = {0}\n" +
                    "            and orders.o_w_id = {1}\n" +
                    "            and orders.o_id >= d_next_o_id - {2}\n" +
                    "            and orders.o_id < d_next_o_id\n" +
                    "    )\n" +
                    "    and t.ol_d_id = {0}\n" +
                    "    and t.ol_w_id= {1}\n" +
                    "    group by t.ol_o_id, t.ol_d_id, t.ol_w_id\n" +
                    ") as Ix\n" +
                    "on order_line.ol_o_id = Ix.ol_o_id\n" +
                    "    and order_line.ol_d_id = Ix.ol_d_id\n" +
                    "    and order_line.ol_w_id = Ix.ol_w_id\n" +
                    "    and order_line.ol_quantity = Ix.max_ol_quantity\n" +
                    "join orders o\n" +
                    "    on order_line.ol_w_id = o.o_w_id and order_line.ol_d_id = o.o_d_id and order_line.ol_o_id = o.o_id\n" +
                    "join customer c\n" +
                    "    on o.o_w_id = c.c_w_id and o.o_d_id = c.c_d_id and o.o_c_id = c.c_id\n" +
                    "join item on order_line.ol_i_id = item.i_id\n" +
                    "group by i_name",
                        d_id, w_id, l
                )
            );
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void execute() {
        try {
            HashMap<String, String> i_name_to_percent = new HashMap<>();
            ResultSet percentage_result = percentage_statement.executeQuery();
            while (percentage_result.next()){
                i_name_to_percent.put(
                    percentage_result.getString("i_name"),
                    percentage_result.getString("percent")
                );
            }

            ResultSet result = data_statement.executeQuery();
            while (result.next()) {
                String d_id = result.getString("order_line.ol_d_id");
                String w_id = result.getString("order_line.ol_w_id");
                String o_id = result.getString("o_id");
                String o_entry_d = result.getString("o_entry_d");
                String c_first = result.getString("c_first");
                String c_middle = result.getString("c_middle");
                String c_last = result.getString("c_last");
                String i_name = result.getString("i_name");
                String ol_quantity = result.getString("ol_quantity");

                System.out.printf(
                    "(D_ID, W_ID, O_ID, O_ENTRY_D, C_FIRST, C_MIDDLE, C_LAST, I_NAME, OL_QUANTITY, percentage): " +
                    "(%s, %s, %s, %s, %s, %s, %s, %s, %s. %s)\n",
                    d_id, w_id, o_id, o_entry_d, c_first,
                    c_middle,c_last,i_name,ol_quantity,
                    i_name_to_percent.get(i_name)
                );
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
