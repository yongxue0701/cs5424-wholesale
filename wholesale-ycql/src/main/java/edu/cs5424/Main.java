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

        if (args.length > 0) {
//            String bathPath = String.format("%s/src/main/resources/scripts/", System.getProperty("user.dir"));
            String bathPath = String.format("./scripts/ycql");
            DataProcessor processor = new DataProcessor(session, bathPath);

            switch (args[0]) {
                case "run":
                    Main main = new Main();
                    for (int i = 0; i <= 19; i++) {
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
            File file = new File(path);    //creates a new file instance
            FileReader fr = new FileReader(file);   // reads the file
            BufferedReader br = new BufferedReader(fr);  // creates a buffering character input stream
            String line;

            while ((line = br.readLine()) != null) {
                String[] parameters = line.split(",");

                switch (parameters[0]) {
//                    case "N":
//                        // Need to handle the multiple-line inputs!
//                        NewOrderTransaction transactionN  = new NewOrderTransaction(session, parameters);
//                        transactionR.execute();
//                        break;
//                    case "P":
//                        PaymentTransaction transactionP = new PaymentTransaction(session, parameters);
//                        transactionP.execute();
//                        break;
//                    case "D":
//                        DeliveryTransaction transactionD = new DeliveryTransaction(session, parameters);
//                        transactionD.execute();
//                        break;
                    case "O":
                        OrderStatusTransaction transactionO = new OrderStatusTransaction(session, parameters);
                        transactionO.execute();
                        break;
                    case "S":
                        StockLevelTransaction transactionS = new StockLevelTransaction(session, parameters);
                        transactionS.execute();
                        break;
//                    case "I":
//                        PopularItemTransaction transactionI = new PopularItemTransaction(session, parameters);
//                        transactionI.execute();
//                        break;
//                    case "T":
//                        TopBalanceTransaction transactionT = new TopBalanceTransaction(session, parameters);
//                        transactionT.execute();
//                        break;
                    case "R":
                        RelatedCustomerTransaction transactionR = new RelatedCustomerTransaction(session, parameters);
                        transactionR.execute();
                        break;
                    default:
                        break;
                }
            }

            fr.close();
            br.close();
            session.close();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
