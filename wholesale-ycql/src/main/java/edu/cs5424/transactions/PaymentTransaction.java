package edu.cs5424.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import java.math.BigDecimal;

public class PaymentTransaction extends BaseTransaction {
    private final int w_id;
    private final int d_id;
    private final int c_id;
    private final BigDecimal payment;
    private BigDecimal w_ytd_amount;
    private BigDecimal d_ytd_amount;
    private float c_ytd_amount;
    private int c_payment_cnt;
    private String w_street_1, w_street_2, w_city, w_state, w_zip;
    private String d_street_1, d_street_2, d_city, d_state, d_zip;
    private String c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit;
    private BigDecimal c_credit_lim, c_discount, c_balance;
    private CqlSession session = null;

    public PaymentTransaction(CqlSession session, String[] params) {
        super(session, params);

        this.session = session;
        this.w_id = Integer.parseInt(params[1]);
        this.d_id = Integer.parseInt(params[2]);
        this.c_id = Integer.parseInt(params[3]);
        this.payment = new BigDecimal(params[4]);
    }

    @Override
    public void execute() {
        System.out.println(String.format("------Payment: warehouse id: %d, district id: %d, customer id: %d------", this.w_id, this.d_id, this.c_id));

        queryWarehouse();
        updateWarehouse();
        queryDistrict();
        updateDistrict();
        queryCustomer();
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

    private void queryWarehouse() {
        String query = String.format("""
                SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_YTD
                FROM Warehouse
                WHERE W_ID = %d
                """, w_id);

        ResultSet rs = this.session.execute(query);
        for (Row row : rs.all()) {
            w_street_1 = row.getString("W_STREET_1");
            w_street_2 = row.getString("W_STREET_2");
            w_city = row.getString("W_CITY");
            w_state = row.getString("W_STATE");
            w_zip = row.getString("W_ZIP");
            w_ytd_amount = row.getBigDecimal("W_YTD");
        }
    }

    private void updateWarehouse() {
        BigDecimal amt = w_ytd_amount.add(payment);
        String query = String.format("""
                UPDATE Warehouse
                SET W_YTD = %.2f
                WHERE W_ID = %d
                """, amt, w_id);
        this.session.execute(query);
    }

    private void queryDistrict() {
        String query = String.format("""
                SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_YTD
                FROM District
                WHERE D_W_ID = %d AND D_ID = %d
                """, w_id, d_id);

        ResultSet rs = this.session.execute(query);
        for (Row row : rs.all()) {
            d_street_1 = row.getString("D_STREET_1");
            d_street_2 = row.getString("D_STREET_2");
            d_city = row.getString("D_CITY");
            d_state = row.getString("D_STATE");
            d_zip = row.getString("D_ZIP");
            d_ytd_amount = row.getBigDecimal("D_YTD");
        }
    }

    private void updateDistrict() {
        BigDecimal amt = d_ytd_amount.add(payment);
        String query = String.format("""
                UPDATE District
                SET D_YTD = %.2f
                WHERE D_W_ID = %d AND D_ID = %d
                """, amt, w_id, d_id);
        this.session.execute(query);
    }

    private void queryCustomer() {
        String query = String.format("""
                SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT
                FROM Customer
                WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d
                """, w_id, d_id, c_id);

        ResultSet rs = this.session.execute(query);
        for (Row row : rs.all()) {
            c_first = row.getString("C_FIRST");
            c_middle = row.getString("C_MIDDLE");
            c_last = row.getString("C_LAST");
            c_street_1 = row.getString("C_STREET_1");
            c_street_2 = row.getString("C_STREET_2");
            c_city = row.getString("C_CITY");
            c_state = row.getString("C_STATE");
            c_zip = row.getString("C_ZIP");
            c_phone = row.getString("C_PHONE");
            c_since = row.getInstant("C_SINCE").toString();
            c_credit = row.getString("C_CREDIT");
            c_credit_lim = row.getBigDecimal("C_CREDIT_LIM");
            c_discount = row.getBigDecimal("C_DISCOUNT");
            c_balance = row.getBigDecimal("C_BALANCE");
            c_ytd_amount = row.getFloat("C_YTD_PAYMENT");
            c_payment_cnt = row.getInt("C_PAYMENT_CNT");
        }
    }

    private void updateCustomer() {
        BigDecimal balance = c_balance.subtract(payment);
        BigDecimal ytd_amt = new BigDecimal(c_ytd_amount);
        BigDecimal amt = ytd_amt.add(payment);
        int paymentCNT = c_payment_cnt + 1;

        String query = String.format("""
                UPDATE Customer
                SET C_BALANCE = %.2f, C_YTD_PAYMENT = %.2f, C_PAYMENT_CNT = %d
                WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d
                """, balance, amt, paymentCNT, w_id, d_id, c_id);
        this.session.execute(query);
    }
}
