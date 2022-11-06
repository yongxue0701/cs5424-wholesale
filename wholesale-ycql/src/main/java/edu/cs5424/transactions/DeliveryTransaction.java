package edu.cs5424.transactions;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.ResultSet;

import java.math.BigDecimal;
import java.util.Date;

public class DeliveryTransaction {
    private final int w_id;
    private final int carrier_id;
    CqlSession session;

    private PreparedStatement order_pstmt;
    private PreparedStatement order_carrier_pstmt;

    private PreparedStatement customer_pstmt;

    private PreparedStatement order_lines_amount_pstmt;
    private PreparedStatement order_lines_pstmt;
    private PreparedStatement customer_update_pstmt;

    public DeliveryTransaction(CqlSession sess, final String[] parameters) {
        session = sess;
        w_id = Integer.parseInt(parameters[1]);
        carrier_id = Integer.parseInt(parameters[2]);

        order_pstmt = sess.prepare(
                "SELECT o_id, o_c_id, o_ol_cnt " +
                        "FROM orders " +
                        "WHERE o_w_id = ? AND o_d_id = ? AND o_carrier_id = -1 " +
                        "LIMIT 1 " +
                        "ALLOW FILTERING;"
        );

        customer_pstmt = sess.prepare(
                "SELECT c_balance, c_delivery_cnt " +
                        "FROM customer " +
                        "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;"
        );
        order_lines_amount_pstmt = sess.prepare(
                "SELECT sum(ol_amount) as sum_ol_amount " +
                        "FROM order_line " +
                        "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;"
        );
        SimpleStatement order_lines =
                SimpleStatement.builder("UPDATE order_line " +
                                "SET ol_delivery_d = ? " +
                                "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_number IN ( ? );")
                        .setConsistencyLevel(DefaultConsistencyLevel.ONE)
                        .build();
        order_lines_pstmt = session.prepare(order_lines);

        SimpleStatement order_carrier =
                SimpleStatement.builder("UPDATE orders " +
                                "SET o_carrier_id = ? " +
                                "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? " +
                                "IF o_carrier_id = -1;")
                        .setConsistencyLevel(DefaultConsistencyLevel.ONE)
                        .build();
        order_carrier_pstmt = sess.prepare(order_carrier);

        SimpleStatement customer_update =
                SimpleStatement.builder("UPDATE customer " +
                                "SET c_balance = ?, c_delivery_cnt = ? " +
                                "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? " +
                                "IF c_delivery_cnt = ?;")
                        .setConsistencyLevel(DefaultConsistencyLevel.ALL)
                        .build();
        customer_update_pstmt = sess.prepare(customer_update);
    }

    public void execute() {
        for (int d_id = 1; d_id <= 10; d_id++) {
            int o_id = -1;
            int c_id = -1;
            int o_ol_cnt = -1;

            // select min id for order needing carrier
            ResultSet order = session.execute(order_pstmt.bind(w_id, d_id));
            if (order != null) {
                Row row = order.all().get(0);
                o_id = row.getInt("o_id");
                c_id = row.getInt("o_c_id");
                o_ol_cnt = row.getBigDecimal("o_ol_cnt").intValue();
            } else continue;
            //update the order with carrier
            session.execute(order_carrier_pstmt.bind(carrier_id, w_id, d_id, o_id));
            //update order-lines with delivery date
            Date ol_delivery_d = new Date();

            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= o_ol_cnt; i++) {
                sb.append(i);
                sb.append(',');
            }
            sb.deleteCharAt(sb.length() - 1);
            session.execute(order_lines_pstmt.bind(ol_delivery_d, w_id, d_id, o_id, sb.toString()));


            //update customer balance and deliver count
            ResultSet customer = session.execute(customer_pstmt.bind(w_id, d_id, c_id));
            if (customer == null) throw new IllegalArgumentException("No matching customer");
            ResultSet amount = session.execute(order_lines_amount_pstmt.bind(w_id, d_id, o_id));
            if (amount == null) throw new IllegalArgumentException("No matching orderline");
            BigDecimal c_balance = amount.all().get(0).getBigDecimal("sum_ol_amount");

            Row customerow = customer.all().get(0);
            c_balance.add(customerow.getBigDecimal("c_balance"));
            int c_delivery_cnt = customerow.getInt("c_delivery_cnt") + 1;

            session.execute(customer_update_pstmt.bind(c_balance, c_delivery_cnt, w_id, d_id, c_id, c_delivery_cnt - 1));

            System.out.printf("(W_ID, D_ID, C_ID, O_ID): (%d, %d, %d, %d)\n",
                    w_id, d_id, o_id, c_id);

        }
    }
}
