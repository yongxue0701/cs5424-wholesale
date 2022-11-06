package edu.cs5424;

import java.net.InetSocketAddress;
import java.io.*;

import com.datastax.oss.driver.api.core.CqlSession;
import edu.cs5424.transactions.*;

public class Main {
    public static void main(String[] args) {
        CqlSession session = CqlSession
                .builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .withKeyspace("wholesale")
                .build();

        if (args.length == 0) {
//            String bathPath = String.format("%s/src/main/resources/scripts/", System.getProperty("user.dir"));
            String bathPath = String.format("./scripts/ycql");
            DataProcessor processor = new DataProcessor(session, bathPath);

            String cmd = "run";
            switch (cmd) {
                case "run":
                    Main main = new Main();
                    for (int i = 0; i < 1; i++) {
//                        String filename = String.format("%s/src/main/resources/xact/%d.txt", System.getProperty("user.dir"), i);
                        String filename = String.format("./xact/%d.txt", i);
                        main.run(session, filename);
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

    public void run(CqlSession session, String path) {
        try {
            BaseTransaction transaction = null;

//            File file = new File(path);    //creates a new file instance
            File file = new File("/Users/y.peng/Desktop/wholesale/project_files/xact_files/testD.txt");    //creates a new file instance
            FileReader fr = new FileReader(file);   // reads the file
            BufferedReader br = new BufferedReader(fr);  // creates a buffering character input stream
            String line;

            while ((line = br.readLine()) != null) {
                String[] parameters = line.split(",");

                switch (parameters[0]) {
                    case "N":
                        // Need to handle the multiple-line inputs!
                        transaction = new NewOrderTransaction(session, br, parameters);
                        break;
                    case "P":
                        transaction = new PaymentTransaction(session, parameters);
                        break;
                    case "D":
                        transaction = new DeliveryTransaction(session, parameters);
                        break;
                    case "O":
                        transaction = new OrderStatusTransaction(session, parameters);
                        break;
                    case "S":
                        transaction = new StockLevelTransaction(session, parameters);
                        break;
                    case "I":
                        transaction = new PopularItemTransaction(session, parameters);
                        break;
                    case "T":
                        transaction = new TopBalanceTransaction(session, parameters);
                        break;
                    case "R":
                        transaction = new RelatedCustomerTransaction(session, parameters);
                        break;
                    default:
                        break;
                }

                if (transaction != null)
                    transaction.execute();
                else
                    System.err.println("Transaction not executed!");
            }

            fr.close();
            br.close();
            session.close();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
