package edu.cs5424;

import java.sql.*;
import java.util.*;
import java.io.*;

public class DataProcessor {
    private Connection conn = null;
    private String basePath;

    public DataProcessor(Connection conn, String basePath) {
        this.conn = conn;
        this.basePath = basePath;
    }

    public void dropTable() throws SQLException {
        System.out.println("------start to drop tables------");
        Statement stmt = this.conn.createStatement();

        try {
            File file = new File(String.format("%s/drop_table.sql", this.basePath));    //creates a new file instance
            Scanner scanner = new Scanner(file);

            while (scanner.hasNextLine()) {
                String query = scanner.nextLine();
                stmt.execute(query);
            }
            System.out.println("------end to drop tables------");
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    public void createTable() throws SQLException {
        System.out.println("------start to create tables------");
        Statement stmt = this.conn.createStatement();

        try {
            File file = new File(String.format("%s/create_table.sql", this.basePath));    //creates a new file instance
            Scanner scanner = new Scanner(file);
            scanner.useDelimiter(";");

            while (scanner.hasNext()) {
                String query = scanner.next() + ";";
                stmt.execute(query);
            }
            System.out.println("------end to create tables------");
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    public void loadData() throws SQLException {
        System.out.println("------start to load data------");
        Statement stmt = this.conn.createStatement();

        stmt.execute(getWarehouseQuery(basePath));
        System.out.println("loaded warehouses");

        stmt.execute(getDistrictQuery(basePath));
        System.out.println("loaded districts");

        stmt.execute(getCustomerQuery(basePath));
        System.out.println("loaded customers");

        stmt.execute(getOrderQuery(basePath));
        System.out.println("loaded orders");

        stmt.execute(getItemQuery(basePath));
        System.out.println("loaded items");

        stmt.execute(getOrderLineQuery(basePath));
        System.out.println("loaded order lines");

        stmt.execute(getStockQuery(basePath));
        System.out.println("loaded order stocks");

        System.out.println("------start to load data------");
    }

    private String getWarehouseQuery(String basePath) {
        String query = "COPY warehouse (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) " +
                "FROM '%s/data/warehouse.csv' " +
                "WITH (DELIMITER ',');";
        return String.format(query, basePath);
    }

    private String getDistrictQuery(String basePath) {
        String query = "COPY district (D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID) " +
                "FROM '%s/data/district.csv' " +
                "WITH (DELIMITER ',');";
        return String.format(query, basePath);
    }

    private String getCustomerQuery(String basePath) {
        String query = "COPY customer (C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, " +
                "                    C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, " +
                "                    C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, " +
                "                    C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, " +
                "                    C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA) " +
                "FROM '%s/data/customer.csv' " +
                "WITH (DELIMITER ',');";
        return String.format(query, basePath);
    }

    private String getOrderQuery(String basePath) {
        String query = "COPY orders (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, " +
                "                    O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) " +
                "    FROM '%s/data/order.csv' " +
                "    WITH (DELIMITER ',', NULL 'null');";
        return String.format(query, basePath);
    }

    private String getItemQuery(String basePath) {
        String query = "COPY item (I_ID, I_NAME, I_PRICE, LIM_ID, I_DATA) " +
                "    FROM '%s/data/item.csv' " +
                "    WITH (DELIMITER ',');";
        return String.format(query, basePath);
    }

    private String getOrderLineQuery(String basePath) {
        String query = "COPY order_Line (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, " +
                "OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, " +
                "OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) " +
                "FROM '%s/data/order-line.csv' " +
                "WITH (DELIMITER ',',  NULL 'null');";
        return String.format(query, basePath);
    }

    private String getStockQuery(String basePath){
        String query = "COPY stock (S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, " +
                "S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, " +
                "S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, " +
                "S_DIST_08, S_DIST_09, S_DIST_10, S_DATA) " +
                "FROM '%s/data/stock.csv' " +
                "WITH (DELIMITER ',');";
        return String.format(query, basePath);
    }
}
