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

        System.out.println("CONNECTED: ");
        System.out.println("session: " + session);

        try {
            File file = new File("/Users/y.peng/Desktop/wholesale/project_files/xact_files/0.txt");    //creates a new file instance
            FileReader fr = new FileReader(file);   // reads the file
            BufferedReader br = new BufferedReader(fr);  // creates a buffering character input stream

            while (br.readLine() != null) {
                String[] parameters = br.readLine().split(",");
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
//                    case "D":
//                        DeliveryTransaction transactionD = new DeliveryTransaction(conn, parameters);
//                        transactionD.execute();
//                        break;
//                    case "O":
//                        OrderStatusTransaction transactionO = new OrderStatusTransaction(conn, parameters);
//                        transactionO.execute();
//                        break;
//                    case "S":
//                        StockLevelTransaction transactionS = new StockLevelTransaction(conn, parameters);
//                        transactionS.execute();
//                        break;
//                    case "I":
//                        PopularItemTransaction transactionI = new PopularItemTransaction(conn, parameters);
//                        transactionI.execute();
//                        break;
//                    case "T":
//                        TopBalanceTransaction transactionT = new TopBalanceTransaction(conn, parameters);
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
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
