package edu.cs5424.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

public class PaymentTransaction {
    private final int w_id;
    private final int d_id;
    private final int c_id;
    private final double payment;
    private String w_street_1, w_street_2, w_city, w_state, w_zip;
    private String d_street_1, d_street_2, d_city, d_state, d_zip;
    private String c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit;
    private double c_credit_lim, c_discount, c_balance;
    private CqlSession session = null;


    public PaymentTransaction(CqlSession session, String[] params) {
        this.session = session;
        this.w_id = Integer.parseInt(params[1]);
        this.d_id = Integer.parseInt(params[2]);
        this.c_id = Integer.parseInt(params[3]);
        this.payment = Double.parseDouble(params[4]);
    }

    public void execute() {
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
    }

    private void queryWarehouse() {
        String query = String.format("""
                SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP
                FROM Warehouse
                WHERE W_ID = %d
                """, w_id);

        ResultSet rs = this.session.execute(query);
        for (Row row : rs.all()) {
            w_street_1 = row.getString(1);
            w_street_2 = row.getString(2);
            w_city = row.getString(3);
            w_state = row.getString(4);
            w_zip = row.getString(5);
        }
    }

    private void updateWarehouse() {
        String query = String.format("""
                UPDATE Warehouse
                SET W_YTD = W_YTD + %f
                WHERE W_ID = %d
                """, payment, w_id);
        this.session.execute(query);
    }

    private void queryDistrict() {
        String query = String.format("""
                SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP
                FROM District
                WHERE D_W_ID = %d AND D_ID = %d
                """, w_id, d_id);

        ResultSet rs = this.session.execute(query);
        for (Row row : rs.all()) {
            d_street_1 = row.getString(1);
            d_street_2 = row.getString(2);
            d_city = row.getString(3);
            d_state = row.getString(4);
            d_zip = row.getString((5));
        }
    }

    private void updateDistrict() {
        String query = String.format("""
                UPDATE District
                SET D_YTD = D_YTD + %f
                WHERE D_W_ID = %d AND D_ID = %d
                """, payment, w_id, d_id);
        this.session.execute(query);
    }

    private void queryCustomer() {
        String query = String.format("""
                SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE
                FROM Customer
                WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d
                """, w_id, d_id, c_id);

        ResultSet rs = this.session.execute(query);
        for (Row row : rs.all()) {
            c_first = row.getString(1);
            c_middle = row.getString(2);
            c_last = row.getString(3);
            c_street_1 = row.getString(4);
            c_street_2 = row.getString(5);
            c_city = row.getString(6);
            c_state = row.getString(7);
            c_zip = row.getString(8);
            c_phone = row.getString(9);
            c_since = row.getString(10);
            c_credit = row.getString(11);
            c_credit_lim = row.getDouble(12);
            c_discount = row.getDouble(13);
            c_balance = row.getDouble(14);
        }
    }

    private void updateCustomer() {
        String query = String.format("""
                UPDATE Customer
                SET C_BALANCE = C_BALANCE - %f, C_YTD_PAYMENT = C_YTD_PAYMENT + %f, C_PAYMENT_CNT = C_PAYMENT_CNT + 1
                WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d
                """, payment, payment, w_id, d_id, c_id);
        this.session.execute(query);
    }

    private void query() {
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

        ResultSet rs = this.session.execute(query);
        for (Row row : rs.all()) {
            c_first = row.getString(1);
            c_middle = row.getString(2);
            c_last = row.getString(3);
            c_street_1 = row.getString(4);
            c_street_2 = row.getString(5);
            c_city = row.getString(6);
            c_state = row.getString(7);
            c_zip = row.getString(8);
            c_phone = row.getString(9);
            c_since = row.getString(10);
            c_credit = row.getString(11);
            c_credit_lim = row.getDouble(12);
            c_discount = row.getDouble(13);
            c_balance = row.getDouble(14);
            d_street_1 = row.getString(15);
            d_street_2 = row.getString(16);
            d_city = row.getString(17);
            d_state = row.getString(18);
            d_zip = row.getString(19);
            w_street_1 = row.getString(20);
            w_street_2 = row.getString(21);
            w_city = row.getString(22);
            w_state = row.getString(23);
            w_zip = row.getString(24);
        }
    }
}
