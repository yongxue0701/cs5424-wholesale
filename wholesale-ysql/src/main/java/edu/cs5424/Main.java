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
            String bathPath = String.format("./scripts/ysql");
            DataProcessor processor = new DataProcessor(conn, bathPath);

            switch (args[0]) {
                case "run":
                    Main main = new Main();
                    for (int i = 0; i < 1; i++) {
                        String filename = String.format("./xact/demo.txt", i);
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
            BaseTransaction transaction = null;

            File file = new File(path);    //creates a new file instance
            FileReader fr = new FileReader(file);   // reads the file
            BufferedReader br = new BufferedReader(fr);  // creates a buffering character input stream
            String line;

            while ((line = br.readLine()) != null) {
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
                        transaction = new DeliveryTransaction(conn, parameters);
                        break;
                    case "O":
                        transaction = new OrderStatusTransaction(conn, parameters);
                        break;
                    case "S":
                        transaction = new StockLevelTransaction(conn, parameters);
                        break;
                    case "I":
                        transaction = new PopularItemTransaction(conn, parameters);
                        break;
                    case "T":
                        transaction = new TopBalanceTransaction(conn, parameters);
                        break;
                    case "R":
                        transaction = new RelatedCustomerTransaction(conn, parameters);
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
            }

            fr.close();
            br.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}