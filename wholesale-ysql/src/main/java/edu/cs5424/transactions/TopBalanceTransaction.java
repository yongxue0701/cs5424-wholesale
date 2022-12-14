package edu.cs5424.transactions;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.ResultSet;

public class TopBalanceTransaction extends BaseTransaction {
    private PreparedStatement statement;

    public TopBalanceTransaction(final Connection conn, final String[] params) {
        super(conn, params);

        try {
            statement = conn.prepareStatement(
                    "select c_first, c_middle, c_last, c_balance, w_name, d_name from (\n" +
                            "    select c_w_id, c_d_id, c_first, c_middle, c_last, c_balance\n" +
                            "    from customer\n" +
                            "    order by c_balance desc\n" +
                            "    limit 10\n" +
                            ") as c_new\n" +
                            "join district d on c_new.c_d_id = d.d_id and c_new.c_w_id = d.d_w_id\n" +
                            "join warehouse on c_new.c_w_id = warehouse.w_id"
            );
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute() {
        System.out.println(String.format("------Top Balance------"));

        ResultSet result = null;
        try {
            result = statement.executeQuery();
            while (result.next()) {
                String c_first = result.getString("c_first");
                String c_middle = result.getString("c_middle");
                String c_last = result.getString("c_last");
                String w_name = result.getString("w_name");
                String d_name = result.getString("d_name");
                BigDecimal c_balance = result.getBigDecimal("c_balance");

                System.out.printf("(C_FIRST, C_MIDDLE, C_LAST, W_NAME, D_NAME, C_BALANCE): (%s, %s, %s, %s, %s, %f)\n",
                        c_first, c_middle, c_last, w_name, d_name, c_balance);
            }

            System.out.println("-----------------------");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}