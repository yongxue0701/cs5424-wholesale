package edu.cs5424.transactions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PaymentTransaction extends BaseTransaction {
    private final int w_id;
    private final int d_id;
    private final int c_id;
    private final double payment;
    private String w_street_1, w_street_2, w_city, w_state, w_zip;
    private String d_street_1, d_street_2, d_city, d_state, d_zip;
    private String c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit;
    private double c_credit_lim, c_discount, c_balance;

    public PaymentTransaction(Connection conn, String[] params) {
        super(conn, params);

        this.w_id = Integer.parseInt(params[1]);
        this.d_id = Integer.parseInt(params[2]);
        this.c_id = Integer.parseInt(params[3]);
        this.payment = Double.parseDouble(params[4]);
    }

    @Override
    public void execute() throws SQLException {
        System.out.println(String.format("------Payment: warehouse id: %d, district id: %d, customer id: %d------", this.w_id, this.d_id, this.c_id));

        query();
//        queryWarehouse();
        updateWarehouse();
//        queryDistrict();
        updateDistrict();
//        queryCustomer();
        updateCustomer();

        System.out.printf("""
                        Payment Transaction Output
                        [1] (C_W_ID: %d, C_D_ID: %d, C_ID: %d)
                            (C_FIRST: %s, C_MIDDLE: %s, C_LAST: %s)
                            (C_STREET_1: %s, C_STREET_2: %s, C_CITY: %s, C_STATE: %s, C_ZIP: %s)
                            C_PHONE: %s, C_SINCE: %s, C_CREDIT: %s, C_CREDIT_LIM: %f, C_DISCOUNT: %f, C_BALANCE: %f
                        [2] (W_STREET_1: %s, W_STREET_2: %s, W_CITY: %s, W_STATE: %s, W_ZIP: %s)
                        [3] (D_STREET_1: %s, D_STREET_2: %s, D_CITY: %s, D_STATE: %s, D_ZIP: %s)
                        [4] PAYMENT: %f
                        """,
                w_id, d_id, c_id,
                c_first, c_middle, c_last,
                c_street_1, c_street_2, c_city, c_state, c_zip,
                c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance,
                w_street_1, w_street_2, w_city, w_state, w_zip,
                d_street_1, d_street_2, d_city, d_state, d_zip,
                payment);

        System.out.println("-----------------------");
    }

    private void queryWarehouse() throws SQLException {
        String query = String.format("""
                SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP
                FROM Warehouse
                WHERE W_ID = %d
                """, w_id);
        ResultSet rs = executeSQLQuery(query);
        rs.next();
        w_street_1 = rs.getString(1);
        w_street_2 = rs.getString(2);
        w_city = rs.getString(3);
        w_state = rs.getString(4);
        w_zip = rs.getString(5);
    }

    private void updateWarehouse() throws SQLException {
        String query = String.format("""
                UPDATE Warehouse
                SET W_YTD = W_YTD + %f
                WHERE W_ID = %d
                """, payment, w_id);
        executeSQL(query);
    }

    private void queryDistrict() throws SQLException {
        String query = String.format("""
                SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP
                FROM District
                WHERE D_W_ID = %d AND D_ID = %d
                """, w_id, d_id);
        ResultSet rs = executeSQLQuery(query);
        rs.next();
        d_street_1 = rs.getString(1);
        d_street_2 = rs.getString(2);
        d_city = rs.getString(3);
        d_state = rs.getString(4);
        d_zip = rs.getString((5));
    }

    private void updateDistrict() throws SQLException {
        String query = String.format("""
                UPDATE District
                SET D_YTD = D_YTD + %f
                WHERE D_W_ID = %d AND D_ID = %d
                """, payment, w_id, d_id);
        executeSQL(query);
    }

    private void queryCustomer() throws SQLException {
        String query = String.format("""
                SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE
                FROM Customer
                WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d
                """, w_id, d_id, c_id);
        ResultSet rs = executeSQLQuery(query);
        rs.next();
        c_first = rs.getString(1);
        c_middle = rs.getString(2);
        c_last = rs.getString(3);
        c_street_1 = rs.getString(4);
        c_street_2 = rs.getString(5);
        c_city = rs.getString(6);
        c_state = rs.getString(7);
        c_zip = rs.getString(8);
        c_phone = rs.getString(9);
        c_since = rs.getString(10);
        c_credit = rs.getString(11);
        c_credit_lim = rs.getDouble(12);
        c_discount = rs.getDouble(13);
        c_balance = rs.getDouble(14);
    }

    private void updateCustomer() throws SQLException {
        String query = String.format("""
                UPDATE Customer
                SET C_BALANCE = C_BALANCE - %f, C_YTD_PAYMENT = C_YTD_PAYMENT + %f, C_PAYMENT_CNT = C_PAYMENT_CNT + 1
                WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d
                """, payment, payment, w_id, d_id, c_id);
        executeSQL(query);
    }

    private void query() throws SQLException {
        String query = String.format("""
                SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP
                    FROM (
                    SELECT C_W_ID, C_D_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE
                    FROM Customer
                    WHERE C_W_ID = %d AND C_D_ID = %d
                    ) AS c_new
                JOIN District d ON c_new.c_d_id = d.d_id AND c_new.c_w_id = d_w_id
                JOIN Warehouse w ON c_new.c_w_id = w.w_id
                """, w_id, d_id);
        ResultSet rs = executeSQLQuery(query);
        rs.next();
        c_first = rs.getString(1);
        c_middle = rs.getString(2);
        c_last = rs.getString(3);
        c_street_1 = rs.getString(4);
        c_street_2 = rs.getString(5);
        c_city = rs.getString(6);
        c_state = rs.getString(7);
        c_zip = rs.getString(8);
        c_phone = rs.getString(9);
        c_since = rs.getString(10);
        c_credit = rs.getString(11);
        c_credit_lim = rs.getDouble(12);
        c_discount = rs.getDouble(13);
        c_balance = rs.getDouble(14);
        d_street_1 = rs.getString(15);
        d_street_2 = rs.getString(16);
        d_city = rs.getString(17);
        d_state = rs.getString(18);
        d_zip = rs.getString(19);
        w_street_1 = rs.getString(20);
        w_street_2 = rs.getString(21);
        w_city = rs.getString(22);
        w_state = rs.getString(23);
        w_zip = rs.getString(24);
    }
}
