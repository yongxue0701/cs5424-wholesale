package edu.cs5424.transactions;

import edu.cs5424.datatype.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.sql.Date;
import java.util.List;

public class NewOrderTransaction extends BaseTransaction {
    private final Connection conn;
    private final BufferedReader br;
    private final int w_id;
    private final int d_id;
    private final int c_id;
    private final int numLines;
    private int n;
    private double d_tax;
    private String c_last;
    private String c_credit;
    private double c_discount;
    private double w_tax;
    private String i_name;
    private double i_price;
    private int s_quantity;
    private String o_entry_d;

    public NewOrderTransaction(final Connection conn, final BufferedReader br, final String[] params) {
        super(conn, params);

        this.conn = conn;
        this.br = br;
        this.c_id = Integer.parseInt(params[1]);
        this.w_id = Integer.parseInt(params[2]);
        this.d_id = Integer.parseInt(params[3]);
        this.numLines = Integer.parseInt(params[4]);
    }

    @Override
    public void execute() throws SQLException {
        System.out.println(String.format("------New Order: warehouse id: %d, district id: %d, customer id: %d, num of lines: %d------", this.w_id, this.d_id, this.c_id, this.numLines));

        queryDistrict(); //get D_NEXT_O_ID (N), D_TAX
        updateDistrict(); //increment D_NEXT_O_ID by 1
        queryClient(); //get C_LAST, C_CREDIT, C_DISCOUNT
        queryWarehouse(); //get W_TAX
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
                    double itemAmount = i_price * ol_quantity;
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

        totalAmount = totalAmount * (1 + d_tax + w_tax) * (1 - c_discount);
        createNewOrder(numItems, allLocal);

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
                n, o_entry_d,
                numItems, totalAmount);
        for (int i = 0; i < orderItems.size(); i++) {
            OrderedItem item = orderItems.get(i);
            System.out.printf("""
                    [5.%d] ITEM_NUMBER: %d, I_NAME: %s, SUPPLIER_WAREHOUSE: %d, QUANTITY: %d, OL_AMOUNT: %f, S_QUANTITY: %d
                    """, i + 1, item.getItemNumber(), item.getItemName(), item.getSupplierWarehouse(), item.getQuantity(), item.getOrderAmount(), item.getStockQuantity());
        }

        System.out.println("-----------------------");
    }

    private void queryDistrict() throws SQLException {
        String query = String.format("""
                SELECT D_TAX, D_NEXT_O_ID
                FROM District
                WHERE D_W_ID = %d AND D_ID = %d;
                """, w_id, d_id);
//        System.out.println(query);
        ResultSet rs = executeSQLQuery(query);
//        debugResultSet(rs);
        while (rs.next()) {
            d_tax = rs.getDouble(1);
            n = rs.getInt(2);
//        System.out.println(d_tax + " " + n);
        }
    }

    private void updateDistrict() throws SQLException {
        String query = String.format("""
                UPDATE District
                SET D_NEXT_O_ID = D_NEXT_O_ID + 1
                WHERE D_W_ID = %d AND D_ID = %d;
                """, w_id, d_id);
        executeSQL(query);
    }

    private void queryClient() throws SQLException {
        String query = String.format("""
                SELECT C_LAST, C_CREDIT, C_DISCOUNT
                FROM Customer
                WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d
                """, w_id, d_id, c_id);
        ResultSet rs = executeSQLQuery(query);
        while (rs.next()) {
            c_last = rs.getString(1);
            c_credit = rs.getString(2);
            c_discount = rs.getDouble(3);
//        System.out.println(c_id + ": " + c_last + " " + c_credit + " " + c_discount);
        }
    }

    private void queryWarehouse() throws SQLException {
        String query = String.format("""
                SELECT W_TAX
                FROM Warehouse
                WHERE W_ID = %d
                """, w_id);
        ResultSet rs = executeSQLQuery(query);
        while (rs.next()) {
            w_tax = rs.getDouble(1);
//        System.out.println(w_id + ": " + w_tax);
        }
    }

    private void queryItem(int i_id) throws SQLException {
        String query = String.format("""
                SELECT I_NAME, I_PRICE
                FROM Item
                WHERE I_ID  = %d
                """, i_id);
        ResultSet rs = executeSQLQuery(query);
        while (rs.next()) {
            i_name = rs.getString(1);
            i_price = rs.getDouble(2);
//        System.out.println(i_id + ": " + i_name + " " + i_price);
        }
    }

    private void queryStock(int i_id) throws SQLException {
        String query = String.format("""
                SELECT S_QUANTITY
                FROM Stock
                WHERE S_W_ID = %d AND S_I_ID = %d
                """, w_id, i_id);
        ResultSet rs = executeSQLQuery(query);
        while (rs.next()) {
            s_quantity = rs.getInt(1);
//        System.out.println(w_id + "," + i_id + ": " + s_quantity);
        }
    }

    private int checkQuantity(int quantity) {
        int adjustedQuantity = s_quantity - quantity;
        if (adjustedQuantity < 10) adjustedQuantity = adjustedQuantity + 100;
        return adjustedQuantity;
    }

    private void updateStock(int adjustedQty, double quantity, boolean isLocal, int i_id) throws SQLException {
        String query;
        if (isLocal) {
            query = String.format("""
                    UPDATE Stock
                    SET S_QUANTITY = %d, S_YTD = S_YTD+%f, S_ORDER_CNT = S_ORDER_CNT+1
                    WHERE S_W_ID = %d AND S_I_ID = %d
                    """, adjustedQty, quantity, w_id, i_id);
        } else {
            query = String.format("""
                    UPDATE Stock
                    SET S_QUANTITY = %d, S_YTD = S_YTD+%f, S_ORDER_CNT = S_ORDER_CNT+1, S_REMOTE_CNT = S_REMOTE_CNT+1
                    WHERE S_W_ID = %d AND S_I_ID = %d
                    """, adjustedQty, quantity, w_id, i_id);
        }
        executeSQL(query);
    }

    private void createNewOrderLine(
            int ol_number,
            int i_id,
            int supply_w_id,
            int quantity,
            double itemAmount)
            throws SQLException {

        String dist_info = String.format("S_DIST_%02d", d_id);

        PreparedStatement pStmt = conn.prepareStatement("""
                INSERT INTO order_Line
                VALUES (?, ?, ?, ?, ?, null, ?, ?, ?, ?)
                """);
        pStmt.setInt(1, w_id);
        pStmt.setInt(2, d_id);
        pStmt.setInt(3, n);
        pStmt.setInt(4, ol_number);
        pStmt.setInt(5, i_id);
        pStmt.setDouble(6, itemAmount);
        pStmt.setInt(7, supply_w_id);
        pStmt.setInt(8, quantity);
        pStmt.setString(9, dist_info);
        pStmt.executeUpdate();
    }

    private void createNewOrder(int numItems, boolean allLocal) throws SQLException {
        int o_all_local = 0;
        if (allLocal) o_all_local = 1;

        Date date = new Date(System.currentTimeMillis());
        o_entry_d = String.valueOf(date);

        PreparedStatement pStmt = conn.prepareStatement("""
                INSERT INTO orders
                VALUES (?, ?, ?, ?, null, ?, ?, ?)
                """);
        pStmt.setInt(1, w_id);
        pStmt.setInt(2, d_id);
        pStmt.setInt(3, n);
        pStmt.setInt(4, c_id);
        pStmt.setInt(5, numItems);
        pStmt.setInt(6, o_all_local);
        pStmt.setDate(7, date);
        pStmt.executeUpdate();
    }

    private void debugResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(",  ");
                String columnValue = resultSet.getString(i);
                System.out.print(i + " " + columnValue + " " + rsmd.getColumnName(i));
            }
            System.out.println("");
        }
    }
}