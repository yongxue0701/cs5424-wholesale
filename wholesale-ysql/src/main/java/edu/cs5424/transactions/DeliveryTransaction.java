package edu.cs5424.transactions;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.ResultSet;

public class DeliveryTransaction extends BaseTransaction {
    private final int w_id;
    private final int carrier_id;
    private Connection conn = null;
    private PreparedStatement order_pstmt;
    private PreparedStatement order_carrier_pstmt;
    private PreparedStatement order_lines_pstmt;
    private PreparedStatement customer_pstmt;
    private PreparedStatement customer_update_pstmt;
    private PreparedStatement order_lines_amount_pstmt;

    public DeliveryTransaction(final Connection conn, final String[] params) {
        super(conn, params);

        this.conn = conn;
        this.w_id = Integer.parseInt(params[1]);
        this.carrier_id = Integer.parseInt(params[2]);

        try {
            order_pstmt = this.conn.prepareStatement(
                    "SELECT o_id, o_c_id, o_ol_cnt " +
                            "FROM orders " +
                            "WHERE o_w_id = ? AND o_d_id = ? AND o_carrier_id IS NULL " +
                            "ORDER BY o_id " +
                            "LIMIT 1;"
            );
            order_carrier_pstmt = this.conn.prepareStatement(
                    "UPDATE orders " +
                            "SET o_carrier_id = ? " +
                            "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? AND o_carrier_id IS NULL;"
            );
            order_lines_pstmt = this.conn.prepareStatement(
                    "UPDATE order_line " +
                            "SET ol_delivery_d = ? " +
                            "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_number <= ?;"
            );
            customer_pstmt = this.conn.prepareStatement(
                    "SELECT c_balance, c_delivery_cnt " +
                            "FROM customer " +
                            "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;"
            );
            order_lines_amount_pstmt = this.conn.prepareStatement(
                    "SELECT sum(ol_amount) as sum_ol_amount " +
                            "FROM order_line " +
                            "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;"
            );
            customer_update_pstmt = this.conn.prepareStatement(
                    "UPDATE customer " +
                            "SET c_balance = ?, c_delivery_cnt = ? " +
                            "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? AND c_delivery_cnt = ?;"
            );
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute() {
        System.out.println(String.format("------Delivery: warehouse id: %d, carrier id: %d------", this.w_id, this.carrier_id));

        PreparedStatement ptmt = null;
        for (int d_id = 1; d_id <= 10; d_id++) {
            int o_id = -1;
            int c_id = -1;
            int o_ol_cnt = -1;
            try {
                // select min id for order needing carrier
                ptmt = order_pstmt;
                ptmt.setInt(1, w_id);
                ptmt.setInt(2, d_id);

                ResultSet order = ptmt.executeQuery();
                if (order == null) {
                    continue;
                }
                while (order.next()) {
                    o_id = order.getInt("o_id");
                    c_id = order.getInt("o_c_id");
                    o_ol_cnt = order.getBigDecimal("o_ol_cnt").intValue();
                }

                //update the order with carrier
                ptmt = order_carrier_pstmt;
                ptmt.setInt(1, carrier_id);
                ptmt.setInt(2, w_id);
                ptmt.setInt(3, d_id);
                ptmt.setInt(4, o_id);
                ptmt.executeUpdate();

                //update order-lines with delivery date
                Date ol_delivery_d = new Date(System.currentTimeMillis());
                ptmt = order_lines_pstmt;
                ptmt.setDate(1, ol_delivery_d);
                ptmt.setInt(2, w_id);
                ptmt.setInt(3, d_id);
                ptmt.setInt(4, o_id);
                ptmt.setInt(5, o_ol_cnt);
                ptmt.executeUpdate();

                //update customer balance and deliver count
                ptmt = customer_pstmt;
                ptmt.setInt(1, w_id);
                ptmt.setInt(2, d_id);
                ptmt.setInt(3, c_id);
                ResultSet customer = ptmt.executeQuery();
                if (customer == null) throw new IllegalArgumentException("No matching customer");

                ptmt = order_lines_amount_pstmt;
                ptmt.setInt(1, w_id);
                ptmt.setInt(2, d_id);
                ptmt.setInt(3, o_id);
                ResultSet amount = ptmt.executeQuery();
                if (amount == null) throw new IllegalArgumentException("No matching orderline");

                BigDecimal c_balance = new BigDecimal(-1);
                int c_delivery_cnt = -1;
                while (amount.next()) {
                    c_balance = amount.getBigDecimal("sum_ol_amount");
                }
                while (customer.next()) {
                    c_balance.add(customer.getBigDecimal("c_balance"));
                    c_delivery_cnt = customer.getInt("c_delivery_cnt") + 1;
                }

                ptmt = customer_update_pstmt;
                ptmt.setBigDecimal(1, c_balance);
                ptmt.setInt(2, c_delivery_cnt);
                ptmt.setInt(3, w_id);
                ptmt.setInt(4, d_id);
                ptmt.setInt(5, c_id);
                ptmt.setInt(6, c_delivery_cnt - 1);
                ptmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            System.out.printf("(W_ID, D_ID, C_ID, O_ID): (%d, %d, %d, %d)\n",
                    w_id, d_id, o_id, c_id);
        }
        System.out.println("-----------------------");
    }
}