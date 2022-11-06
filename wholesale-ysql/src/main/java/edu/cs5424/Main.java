package edu.cs5424;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.io.*;

import edu.cs5424.transactions.*;

public class Main {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5433/yugabyte",
                "yugabyte",
                "yugabyte");

        if (args.length > 0) {
//            String bathPath = String.format("%s/src/main/resources/scripts/", System.getProperty("user.dir"));
            String bathPath = String.format("./scripts/");
            DataProcessor processor = new DataProcessor(conn, bathPath);

            switch (args[0]) {
                case "run":
                    Main main = new Main();
                    for (int i = 0; i <= 19; i++) {
//                        String filename = String.format("%s/src/main/resources/xact/%d.txt", System.getProperty("user.dir"), i);
                        String filename = String.format("./xact/%d.txt", i);
                        main.run(conn, filename);
                    }
                    break;
                case "create":
                    processor.createTable();
                    break;
                case "drop":
                    processor.dropTable();
                    break;
                case "load":
                    processor.loadData();
                    break;
            }
        }
    }

    public void run(Connection conn, String path) throws SQLException {
        try {
            File file = new File(path);    //creates a new file instance
            FileReader fr = new FileReader(file);   // reads the file
            BufferedReader br = new BufferedReader(fr);  // creates a buffering character input stream
            String line;

            while ((line = br.readLine()) != null) {
                String[] parameters = line.split(",");
                switch (parameters[0]) {
//                    case "N":
//                        // Need to handle the multiple-line inputs!
//                        NewOrderTransaction transactionN  = new NewOrderTransaction(conn, parameters);
//                        transactionR.execute();
//                        break;
//                    case "P":
//                        PaymentTransaction transactionP = new PaymentTransaction(conn, parameters);
//                        transactionP.execute();
//                        break;
                    case "D":
                        DeliveryTransaction transactionD = new DeliveryTransaction(conn, parameters);
                        transactionD.execute();
                        break;
                    case "O":
                        OrderStatusTransaction transactionO = new OrderStatusTransaction(conn, parameters);
                        transactionO.execute();
                        break;
                    case "S":
                        StockLevelTransaction transactionS = new StockLevelTransaction(conn, parameters);
                        transactionS.execute();
                        break;
                    case "I":
                        PopularItemTransaction transactionI = new PopularItemTransaction(conn, parameters);
                        transactionI.execute();
                        break;
                    case "T":
                        TopBalanceTransaction transactionT = new TopBalanceTransaction(conn, parameters);
                        transactionT.execute();
                        break;
                    case "R":
                        RelatedCustomerTransaction transactionR = new RelatedCustomerTransaction(conn, parameters);
                        transactionR.execute();
                        break;
                    default:
                        break;
                }
            }

            fr.close();
            br.close();
            conn.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}