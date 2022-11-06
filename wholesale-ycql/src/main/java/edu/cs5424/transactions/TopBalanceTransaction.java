package edu.cs5424.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import java.math.BigDecimal;
import java.util.stream.Collectors;

public class TopBalanceTransaction extends BaseTransaction {

    private CqlSession session = null;

    public TopBalanceTransaction(final CqlSession session, final String[] params) {
        super(session, params);

        this.session = session;
    }

    class Customer {
        public Integer c_w_id;
        public Integer c_d_id;
        public String c_first;
        public String c_middle;
        public String c_last;
        public BigDecimal c_balance;

        public Customer(Integer c_w_id, Integer c_d_id, String c_first, String c_middle, String c_last, BigDecimal c_balance) {
            this.c_w_id = c_w_id;
            this.c_d_id = c_d_id;
            this.c_first = c_first;
            this.c_middle = c_middle;
            this.c_last = c_last;
            this.c_balance = c_balance;
        }
    }

    class CustomerComparator implements Comparator<Customer> {
        @Override
        public int compare(Customer o1, Customer o2) {
            return o1.c_balance.compareTo(o2.c_balance);
        }
    }

    @Override
    public void execute() {
        PriorityQueue<Customer> customers = new PriorityQueue<>(new CustomerComparator());

        ResultSet customerResult = this.session.execute(
                "select c_w_id, c_d_id, c_first, c_middle, c_last, c_balance\n" +
                        "    from wholesale.customer"
        );
        for (Row row : customerResult.all()) {
            if (customers.size() >= 10) {
                customers.poll();
            }
            customers.offer(new Customer(
                    row.getInt("c_w_id"), row.getInt("c_d_id"),
                    row.getString("c_first"), row.getString("c_middle"),
                    row.getString("c_last"), row.getBigDecimal("c_balance")
            ));
        }

        // fetch warehouse
        String wIDList = customers.stream()
                .map(c -> c.c_w_id.toString())
                .collect(Collectors.joining(","));

        HashMap<Integer, String> warehouseIdToName = new HashMap<>();
        ResultSet warehouseResult = this.session.execute(
                String.format(
                        "select w_id, w_name from wholesale.warehouse where w_id in (%s)",
                        wIDList
                )
        );
        for (Row row : warehouseResult.all()) {
            warehouseIdToName.put(row.getInt("w_id"), row.getString("w_name"));
        }

        // fetch district
        HashMap<Map.Entry<Integer, Integer>, String> districtIdToName = new HashMap<>();
        customers.stream()
                .map(c -> String.format("d_w_id = %s and d_id = %s", c.c_w_id, c.c_d_id))
                .forEach(condition -> {
                    ResultSet res = this.session.execute(
                            String.format("select d_w_id, d_id, d_name from wholesale.district where %s", condition)
                    );

                    for (Row row : res.all()) {
                        districtIdToName.put(
                                Map.entry(row.getInt("d_w_id"), row.getInt("d_id")),
                                row.getString("d_name")
                        );
                    }
                });

        for (Customer c : customers) {
            System.out.printf("(C_FIRST, C_MIDDLE, C_LAST, W_NAME, D_NAME, C_BALANCE): (%s, %s, %s, %s, %s, %.2f)\n",
                    c.c_first, c.c_middle, c.c_last,
                    warehouseIdToName.get(c.c_w_id), districtIdToName.get(Map.entry(c.c_w_id, c.c_d_id)),
                    c.c_balance);
        }
    }
}