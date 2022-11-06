package edu.cs5424.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import edu.cs5424.datatype.OrderedItem;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;

public class NewOrderTransaction extends BaseTransaction {
    private final CqlSession session;
    private final BufferedReader br;
    private final int w_id;
    private final int d_id;
    private final int c_id;
    private final int numLines;
    private int order_id;
    private BigDecimal d_tax;
    private String c_last;
    private String c_credit;
    private BigDecimal c_discount;
    private BigDecimal w_tax;
    private String i_name;
    private BigDecimal i_price;
    private BigDecimal s_quantity;
    private String o_entry_d;
    private BigDecimal s_ytd;

    public NewOrderTransaction(final CqlSession session, final BufferedReader br, final String[] params) {
        super(session, params);

        this.session = session;
        this.br = br;
        this.c_id = Integer.parseInt(params[1]);
        this.w_id = Integer.parseInt(params[2]);
        this.d_id = Integer.parseInt(params[3]);
        this.numLines = Integer.parseInt(params[4]);
    }

    @Override
    public void execute() {
        System.out.println(String.format("------New Order: warehouse id: %d, district id: %d, customer id: %d, num of lines: %d------", this.w_id, this.d_id, this.c_id, this.numLines));

        queryDistrict(); //get D_NEXT_O_ID (N), D_TAX
        updateDistrict(); //increment D_NEXT_O_ID by 1
        queryClient(); //get C_LAST, C_CREDIT, C_DISCOUNT
        queryWarehouse(); //get W_TAX
        createNewOrderTemp();
        createNewOrderByTimestamp();

        boolean allLocal = true;
        int numItems = 0;
        double totalAmount = 0;

        List<OrderedItem> orderItems = new ArrayList<>();
        for (int i = 0; i < numLines; i++) {
            try {
                String line = br.readLine();
                if (line != null) {
                    String[] parameters = line.split(",");
                    int ol_i_id = Integer.parseInt(parameters[0]);
                    int ol_supply_w_id = Integer.parseInt(parameters[1]);
                    int ol_quantity = Integer.parseInt(parameters[2]);
                    boolean isLocal = true;

                    queryItem(ol_i_id); //get I_NAME, I_PRICE
                    queryStock(ol_i_id); //get S_QUANTITY
                    int adjustedQty = checkQuantity(ol_quantity);
                    if (ol_supply_w_id != w_id) {
                        allLocal = false;
                        isLocal = false;
                    }
                    updateStock(adjustedQty, ol_quantity, isLocal, ol_i_id);

                    numItems = numItems + ol_quantity;
                    double itemAmount = i_price.multiply(new BigDecimal(ol_quantity)).doubleValue();
                    totalAmount = totalAmount + itemAmount;
                    createNewOrderLine(i + 1, ol_i_id, ol_supply_w_id, ol_quantity, itemAmount);
                    orderItems.add(
                            new OrderedItem(
                                    ol_i_id,
                                    i_name,
                                    ol_supply_w_id,
                                    ol_quantity,
                                    itemAmount,
                                    adjustedQty));
                } else {
                    System.err.printf("""
                            [NewTransactionOrder]
                            Out of bounds!
                            1st line: [N,%d,%d,%d,%d]
                            Failed line: %d""", w_id, d_id, c_id, numLines, i + 1);
                }

            } catch (IOException e) {
                System.out.printf("""
                        [NewTransactionOrder]
                        Error reading intermediate line!
                        1st line: [N,%d,%d,%d,%d]
                        Failed line: %d""", w_id, d_id, c_id, numLines, i + 1);
                throw new RuntimeException(e);
            }
        }

        BigDecimal totalTax = d_tax.add(w_tax).add(new BigDecimal(1));
        BigDecimal discount = new BigDecimal(1).subtract(c_discount);

        totalAmount = totalAmount * totalTax.doubleValue() * discount.doubleValue();
        updateOrder(numItems, allLocal);

        System.out.printf("""
                        New Order Transaction Output
                        [1] (W_ID: %d, D_ID: %d, C_ID, %d)
                            C_LAST: %s, C_CREDIT: %s, C_DISCOUNT: %f
                        [2] W_TAX: %f, D_TAX: %f
                        [3] O_ID: %d, O_ENTRY_D: %s
                        [4] NUM_ITEMS: %d, TOTAL_AMOUNT: %f
                        """,
                w_id, d_id, c_id,
                c_last, c_credit, c_discount,
                w_tax, d_tax,
                order_id, o_entry_d,
                numItems, totalAmount);
        for (int i = 0; i < orderItems.size(); i++) {
            OrderedItem item = orderItems.get(i);
            System.out.printf("""
                    [5.%d] ITEM_NUMBER: %d, I_NAME: %s, SUPPLIER_WAREHOUSE: %d, QUANTITY: %d, OL_AMOUNT: %f, S_QUANTITY: %d
                    """, i + 1, item.getItemNumber(), item.getItemName(), item.getSupplierWarehouse(), item.getQuantity(), item.getOrderAmount(), item.getStockQuantity());
        }

        System.out.println("-----------------------");
    }

    private void queryDistrict() {
        String query = String.format("""
                SELECT D_TAX, D_NEXT_O_ID
                FROM District
                WHERE D_W_ID = %d AND D_ID = %d;
                """, w_id, d_id);

        ResultSet rs = this.session.execute(query);
        for (Row row : rs.all()) {
            d_tax = row.getBigDecimal("D_TAX");
            order_id = row.getInt("D_NEXT_O_ID");
//        System.out.println(d_tax + " " + n);
        }
    }

    private void updateDistrict() {
        String query = String.format("""
                UPDATE District
                SET D_NEXT_O_ID = D_NEXT_O_ID + 1
                WHERE D_W_ID = %d AND D_ID = %d;
                """, w_id, d_id);
        this.session.execute(query);
    }

    private void queryClient() {
        String query = String.format("""
                SELECT C_LAST, C_CREDIT, C_DISCOUNT
                FROM Customer
                WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d
                """, w_id, d_id, c_id);

        ResultSet rs = this.session.execute(query);
        for (Row row : rs.all()) {
            c_last = row.getString("C_LAST");
            c_credit = row.getString("C_CREDIT");
            c_discount = row.getBigDecimal("C_DISCOUNT");
//        System.out.println(c_id + ": " + c_last + " " + c_credit + " " + c_discount);
        }
    }

    private void queryWarehouse() {
        String query = String.format("""
                SELECT W_TAX
                FROM Warehouse
                WHERE W_ID = %d
                """, w_id);

        ResultSet rs = this.session.execute(query);
        for (Row row : rs.all()) {
            w_tax = row.getBigDecimal("W_TAX");
//        System.out.println(w_id + ": " + w_tax);
        }
    }

    private void queryItem(int i_id) {
        String query = String.format("""
                SELECT I_NAME, I_PRICE
                FROM Item
                WHERE I_ID  = %d
                """, i_id);

        ResultSet rs = this.session.execute(query);
        for (Row row : rs.all()) {
            i_name = row.getString("I_NAME");
            i_price = row.getBigDecimal("I_PRICE");
//        System.out.println(i_id + ": " + i_name + " " + i_price);
        }
    }

    private void queryStock(int i_id) {
        String query = String.format("""
                SELECT S_QUANTITY, S_YTD
                FROM Stock
                WHERE S_W_ID = %d AND S_I_ID = %d
                """, w_id, i_id);

        ResultSet rs = this.session.execute(query);
        for (Row row : rs.all()) {
            s_quantity = row.getBigDecimal("S_QUANTITY");
            s_ytd = row.getBigDecimal("S_YTD");
//        System.out.println(w_id + "," + i_id + ": " + s_quantity);
        }
    }

    private int checkQuantity(int quantity) {
        int adjustedQuantity = s_quantity.subtract(new BigDecimal(quantity)).intValue();
        if (adjustedQuantity < 10) adjustedQuantity = adjustedQuantity + 100;
        return adjustedQuantity;
    }

    private void updateStock(int adjustedQty, double quantity, boolean isLocal, int i_id) {
        BigDecimal qty = s_ytd.add(new BigDecimal(quantity));
        String query;
        if (isLocal) {
            query = String.format("""
                    UPDATE Stock
                    SET S_QUANTITY = %d, S_YTD = %.2f, S_ORDER_CNT = S_ORDER_CNT+1
                    WHERE S_W_ID = %d AND S_I_ID = %d
                    """, adjustedQty, qty, w_id, i_id);
        } else {
            query = String.format("""
                    UPDATE Stock
                    SET S_QUANTITY = %d, S_YTD = %.2f, S_ORDER_CNT = S_ORDER_CNT+1, S_REMOTE_CNT = S_REMOTE_CNT+1
                    WHERE S_W_ID = %d AND S_I_ID = %d
                    """, adjustedQty, qty, w_id, i_id);
        }
        this.session.execute(query);
    }

    private void createNewOrderLine(int ol_number, int i_id, int supply_w_id, int quantity, double itemAmount) {
        String dist_info = String.format("S_DIST_%02d", d_id);

        PreparedStatement pStmt = this.session.prepare("""
                INSERT INTO order_Line (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO)
                VALUES (?, ?, ?, ?, ?, null, ?, ?, ?, ?)
                """);
        this.session.execute(pStmt.bind(w_id, d_id, order_id, ol_number, i_id, new BigDecimal(itemAmount), supply_w_id, new BigDecimal(quantity), dist_info));
    }

    private void createNewOrderTemp() {
        Date date = new Date(System.currentTimeMillis());
        o_entry_d = String.valueOf(date);

        PreparedStatement pStmt = this.session.prepare("""
                INSERT INTO orders (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D)
                VALUES (?, ?, ?, ?, null, null, null, (dateof(now())))
                """);
        this.session.execute(pStmt.bind(w_id, d_id, order_id, c_id));
    }

    private void createNewOrderByTimestamp() {
        PreparedStatement pStmt = this.session.prepare("""
                INSERT INTO Orders_by_timestamp (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D)
                VALUES (?, ?, ?, ?, null, null, null, (dateof(now())))
                """);
        this.session.execute(pStmt.bind(w_id, d_id, order_id, c_id));
    }

    private void updateOrder(int numItems, boolean allLocal) {
        int o_all_local = 0;
        if (allLocal) o_all_local = 1;

        String query = String.format("""
                UPDATE Orders
                SET O_OL_CNT = %d, O_ALL_LOCAL = %d
                WHERE O_W_ID = %d AND O_D_ID = %d AND O_ID = %d IF O_C_ID = %d
                """, numItems, o_all_local, w_id, d_id, order_id, c_id);
        this.session.execute(query);
    }
}
