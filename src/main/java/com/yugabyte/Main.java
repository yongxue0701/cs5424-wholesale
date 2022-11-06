package com.yugabyte;

import com.yugabyte.transactions.BaseTransaction;
import com.yugabyte.transactions.NewOrderTransaction;
import com.yugabyte.transactions.PaymentTransaction;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.*;
import java.sql.*;
import java.util.Scanner;


public class Main {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://127.0.1.1:5433/yugabyte",
                "yugabyte",
                "yugabyte");
        Statement stmt = conn.createStatement();
        System.out.println("Connected to the YugabyteDB Cluster successfully.");
//        setUpDatabase(stmt);
//        testData(stmt);

        BaseTransaction transaction = null;
        try {
//            Scanner scanner = new Scanner(System.in);
            File file = new File("/home/eric/Desktop/EricTransactionSQL/src/main/resources/testNewOrderTransaction.txt");
//            File file = new File("/home/eric/Desktop/EricTransactionSQL/src/main/resources/testPaymentTransaction.txt");
//            File file = new File("/home/eric/Desktop/EricTransactionSQL/src/main/resources/0.txt");
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);

            String line = br.readLine();
            while (line != null) {
                String[] parameters = line.split(",");
                switch (parameters[0]) {
                    case "N":
                        // Need to handle the multiple-line inputs!
                        transaction = new NewOrderTransaction(conn, br, parameters);
                        break;
                    case "P":
                        transaction = new PaymentTransaction(conn, parameters);
                        break;
                    case "D":
//                            DeliveryTransaction transaction = new DeliveryTransaction(conn, parameters);
                        break;
                    case "O":
//                            OrderStatusTransaction transaction = new OrderStatusTransaction(conn, parameters);
                        break;
                    case "S":
//                            StockLevelTransaction transaction = new StockLevelTransaction(conn, parameters);
                        break;
//                    case "I":
//                        PopularItemTransaction transaction = new PopularItemTransaction(conn, parameters);
//                        break;
//                    case "T":
//                        TopBalanceTransaction transaction = new TopBalanceTransaction(conn, parameters);
//                        break;
                    case "R":
//                            RelatedCustomerTransaction transaction = new RelatedCustomerTransaction(conn, parameters);
                        break;
                    default:
                        System.err.println("Unrecognised transaction encountered!");
                        transaction = null;
                        break;
                }

                if (transaction != null)
                    transaction.execute();
                else
                    System.err.println("Transaction not executed!");

                line = br.readLine();
            }

            fr.close();
            br.close();

        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    //remember to remove drop table statements on actual cloud server
    private static void setUpDatabase(Statement stmt) throws SQLException {
        stmt.execute("DROP TABLE IF EXISTS Warehouse");
        stmt.execute("CREATE TABLE IF NOT EXISTS Warehouse (W_ID integer PRIMARY KEY,W_NAME varchar(10),W_STREET_1 varchar(20),W_STREET_2 varchar(20),W_CITY varchar(20),W_STATE char(2),W_ZIP char(9),W_TAX decimal(4,4),W_YTD decimal(12,2));");
        stmt.execute("COPY warehouse (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) " +
                "FROM '/home/eric/Desktop/EricTransactionSQL/src/main/resources/warehouse.csv' " + //remember to change this dir
                "WITH (delimiter',');");

        stmt.execute("DROP TABLE IF EXISTS district;");
        stmt.execute("CREATE TABLE IF NOT EXISTS District (D_W_ID integer,D_ID integer,D_NAME varchar(10),D_STREET_1 varchar(20),D_STREET_2 varchar(20),D_CITY varchar(20),D_STATE char(2),D_ZIP char(9),D_TAX decimal(4,4),D_YTD decimal(12,2),D_NEXT_O_ID integer);");
        stmt.execute("COPY district (D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID) " +
                "    FROM '/home/eric/Desktop/EricTransactionSQL/src/main/resources/district.csv'" +
                "    WITH (DELIMITER ',');");

        stmt.execute("DROP TABLE IF EXISTS customer;");
        stmt.execute("CREATE TABLE IF NOT EXISTS Customer (C_W_ID integer,C_D_ID integer,C_ID integer,C_FIRST varchar(16),C_MIDDLE char(2),C_LAST varchar(16),C_STREET_1 varchar(20),C_STREET_2 varchar(20),C_CITY varchar(20),C_STATE char(2),C_ZIP char(9),C_PHONE char(16),C_SINCE timestamp,C_CREDIT CHAR(2),C_CREDIT_LIM decimal(12,2),C_DISCOUNT decimal(5,4),C_BALANCE decimal(12,2),C_YTD_PAYMENT float,C_PAYMENT_CNT integer,C_DELIVERY_CNT integer,C_DATA varchar(500));");
        stmt.execute("COPY customer (C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT,C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA) " +
                "    FROM '/home/eric/Desktop/EricTransactionSQL/src/main/resources/customer.csv' " +
                "    WITH (DELIMITER ',');");

        stmt.execute("DROP TABLE IF EXISTS orders;");
        stmt.execute("CREATE TABLE IF NOT EXISTS Orders (O_W_ID integer,O_D_ID integer,O_ID integer,O_C_ID integer,O_CARRIER_ID integer,O_OL_CNT decimal(2,0),O_ALL_LOCAL decimal(1,0),O_ENTRY_D timestamp);");
        stmt.execute("COPY orders (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) " +
                "    FROM '/home/eric/Desktop/EricTransactionSQL/src/main/resources/order.csv' " +
                "    WITH (DELIMITER ',', NULL 'null');");

        stmt.execute("DROP TABLE IF EXISTS item;");
        stmt.execute("CREATE TABLE IF NOT EXISTS Item (I_ID integer PRIMARY KEY,I_NAME varchar(24),I_PRICE decimal(5,2),LIM_ID integer,I_DATA varchar(50));");
        stmt.execute("COPY item (I_ID, I_NAME, I_PRICE, LIM_ID, I_DATA) " +
                "    FROM '/home/eric/Desktop/EricTransactionSQL/src/main/resources/item.csv' " +
                "    WITH (DELIMITER ',');");

        stmt.execute("DROP TABLE IF EXISTS order_Line;");
        stmt.execute("CREATE TABLE IF NOT EXISTS Order_Line (OL_W_ID integer,OL_D_ID integer,OL_O_ID integer,OL_NUMBER integer,OL_I_ID integer,OL_DELIVERY_D timestamp,OL_AMOUNT decimal(7,2),OL_SUPPLY_W_ID integer,OL_QUANTITY decimal(2,0),OL_DIST_INFO char(24));");
        stmt.execute("COPY order_Line (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER,OL_I_ID, OL_DELIVERY_D, OL_AMOUNT,OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) " +
                "    FROM '/home/eric/Desktop/EricTransactionSQL/src/main/resources/order-line.csv' " +
                "    WITH (DELIMITER ',',  NULL 'null');");

        stmt.execute("DROP TABLE IF EXISTS stock;");
        stmt.execute("CREATE TABLE IF NOT EXISTS Stock (S_W_ID integer,S_I_ID integer,S_QUANTITY decimal(4,0),S_YTD decimal(8,2),S_ORDER_CNT integer,S_REMOTE_CNT integer,S_DIST_01 char(24),S_DIST_02 char(24),S_DIST_03 char(24),S_DIST_04 char(24),S_DIST_05 char(24),S_DIST_06 char(24),S_DIST_07 char(24),S_DIST_08 char(24),S_DIST_09 char(24),S_DIST_10 char(24),S_DATA varchar(50));");
        stmt.execute("COPY stock (S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT,S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03,S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07,S_DIST_08, S_DIST_09, S_DIST_10, S_DATA) " +
                "    FROM '/home/eric/Desktop/EricTransactionSQL/src/main/resources/stock.csv' " +
                "    WITH (DELIMITER ',');");
    }

    private static void testData(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("select w_id, w_name from warehouse");
        while (rs.next()) {
            System.out.printf("Query returned: w_id = %d, w_name = %s%n",
                    rs.getInt(1), rs.getString(2));
        }
    }
}