package edu.cs5424.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.*;
import java.util.stream.Collectors;

public class PopularItemTransaction {
    private CqlSession session = null;
    private Integer w_id = null;
    private Integer d_id = null;
    private Integer l = null;

    class OrderLine {
        public Integer ol_o_id;
        public Integer ol_d_id;
        public Integer ol_w_id;
        public Integer ol_quantity;
        public Integer ol_i_id;

        public OrderLine(Integer ol_o_id, Integer ol_d_id, Integer ol_w_id, Integer ol_quantity, Integer ol_i_id) {
            this.ol_o_id = ol_o_id;
            this.ol_d_id = ol_d_id;
            this.ol_w_id = ol_w_id;
            this.ol_quantity = ol_quantity;
            this.ol_i_id = ol_i_id;
        }
    }

    class Customer {
        public String c_first;
        public String c_middle;
        public String c_last;

        public Customer(String c_first, String c_middle, String c_last) {
            this.c_first = c_first;
            this.c_middle = c_middle;
            this.c_last = c_last;
        }
    }

    public PopularItemTransaction(final CqlSession session, final String[] parameters) {
        this.w_id = Integer.parseInt(parameters[1]);
        this.d_id = Integer.parseInt(parameters[2]);
        // number of last orders
        this.l = Integer.parseInt(parameters[3]);
        this.session = session;
    }

    public void execute() {
        ResultSet districtRes = this.session.execute(
                String.format(
                        "select d_next_o_id from wholesale.district where (d_w_id = %d and d_id = %d)",
                        this.w_id, this.d_id
                )
        );
        Integer d_next_o_id = null;
        for (Row row : districtRes.all()) {
            d_next_o_id = row.getInt("d_next_o_id");
        }
        if (d_next_o_id == null) {
            throw new RuntimeException(String.format("can not get d_next_o_id with (w_id, d_id): (%d, %d)", this.w_id, this.d_id));
        }

        ResultSet SetSForOrders = this.session.execute(
                String.format(
                        "select o_id from wholesale.orders\n" +
                                "where o_d_id = %d and o_w_id = %d and o_id >= %d - %d and o_id < %d",
                        this.d_id, this.w_id, d_next_o_id, this.l, d_next_o_id
                )
        );
        List<String> setSOrderIDList = new ArrayList<>();
        for (Row row : SetSForOrders.all()) {
            setSOrderIDList.add(
                    Integer.toString(row.getInt("o_id"))
            );
        }

        ResultSet orderLinesRes = this.session.execute(
                String.format(
                        "select ol_o_id, ol_d_id, ol_w_id, ol_quantity from wholesale.order_line\n" +
                                "where ol_o_id in (%s) and ol_d_id = %d and ol_w_id= %d",
                        String.join(",", setSOrderIDList), this.d_id, this.w_id
                )
        );
        HashMap<List<Integer>, Integer> olToMaxQuantity = new HashMap<>();
        for (Row row : orderLinesRes.all()) {
            Integer ol_o_id = row.getInt("ol_o_id");
            Integer ol_d_id = row.getInt("ol_d_id");
            Integer ol_w_id = row.getInt("ol_w_id");
            Integer ol_quantity = row.getBigDecimal("ol_quantity").intValue();

            List<Integer> key = Arrays.asList(ol_o_id, ol_d_id, ol_w_id);
            if (!olToMaxQuantity.containsKey(key)) {
                olToMaxQuantity.put(key, ol_quantity);
                continue;
            }
            Integer val = olToMaxQuantity.get(key);
            if (val < ol_quantity) {
                olToMaxQuantity.put(key, val);
            }
        }

        // final order lines result
        List<OrderLine> orderLinesResult = new ArrayList<>();

        String ol_o_id_condition = olToMaxQuantity.keySet()
                .stream().map(k -> k.get(0).toString())
                .collect(Collectors.joining(","));
        String ol_quantity_condition = olToMaxQuantity.values()
                .stream().map(Object::toString)
                .collect(Collectors.joining(","));

        ResultSet finalOrderLinesRes = this.session.execute(
                String.format(
                        "select * from wholesale.order_line\n" +
                                "where ol_o_id in (%s) and ol_d_id = %d and ol_w_id = %d and ol_quantity in (%s)",
                        ol_o_id_condition, this.d_id, this.w_id, ol_quantity_condition
                )
        );

        // ol_o_id -> ol_quantity
        HashMap<Integer, List<Integer>> tempRes = new HashMap<>();
        for (Row row : finalOrderLinesRes.all()) {
            tempRes.put(
                    row.getInt("ol_o_id"),
                    Arrays.asList(
                            row.getBigDecimal("ol_quantity").intValue(), row.getInt("ol_i_id")
                    )
            );
        }

        olToMaxQuantity.forEach((k, v) -> {
            orderLinesResult.add(new OrderLine(
                    k.get(0), // ol_o_id
                    this.d_id, // ol_d_id
                    this.w_id, // ol_w_id
                    tempRes.get(k.get(0)).get(0), // ol_quantity
                    tempRes.get(k.get(0)).get(1) // ol_i_id
            ));
        });

        // related orders
        HashMap<Integer, String> oIdToOEntryD = new HashMap<>();
        HashMap<Integer, String> oIdToOcId = new HashMap<>();  // customer id
        for (
                Row row : this.session.execute(
                String.format(
                        "select * from wholesale.orders\n" +
                                "where o_w_id = %s and o_d_id = %s and o_id in (%s)",
                        this.w_id, this.d_id,
                        orderLinesResult.stream().map(ol -> ol.ol_o_id.toString())
                                .collect(Collectors.joining(","))
                )
        ).all()
        ) {
            oIdToOEntryD.put(row.getInt("o_id"), row.getInstant("o_entry_d").toString());
            oIdToOcId.put(row.getInt("o_id"), Integer.toString(row.getInt("o_c_id")));
        }

        // related customers
        HashMap<String, Customer> cIdToCustomer = new HashMap<>();  // customer id
        for (
                Row row : this.session.execute(
                String.format(
                        "select * from wholesale.customer\n" +
                                "where c_w_id = %s and c_d_id = %s and c_id in (%s)",
                        this.w_id, this.d_id, String.join(",", oIdToOcId.values())
                )
        ).all()
        ) {
            cIdToCustomer.put(
                    Integer.toString(row.getInt("c_id")), new Customer(
                            row.getString("c_first"),
                            row.getString("c_middle"),
                            row.getString("c_last")
                    )
            );
        }

        // related items
        HashMap<String, String> iIdToItemName = new HashMap<>();  // item id
        for (
                Row row : this.session.execute(
                String.format(
                        "select * from wholesale.item\n" +
                                "where i_id in (%s)",
                        orderLinesResult.stream().map(ol -> ol.ol_i_id.toString())
                                .collect(Collectors.joining(","))
                )
        ).all()
        ) {
            iIdToItemName.put(
                    Integer.toString(row.getInt("i_id")),
                    row.getString("i_name")
            );
        }

        // percentage
        HashMap<Integer, Double> itemIdOrderCounter = new HashMap<>();
        for (OrderLine orderLine : orderLinesResult) {
            if (!itemIdOrderCounter.containsKey(orderLine.ol_i_id)) {
                itemIdOrderCounter.put(orderLine.ol_i_id, 1.0);
                continue;
            }
            itemIdOrderCounter.put(
                    orderLine.ol_i_id,
                    itemIdOrderCounter.get(orderLine.ol_i_id) + 1
            );
        }

        for (OrderLine ol : orderLinesResult) {
            Customer c = cIdToCustomer.get(oIdToOcId.get(ol.ol_o_id));
            System.out.printf(
                    "(D_ID, W_ID, L, O_ID, O_ENTRY_D, C_FIRST, C_MIDDLE, C_LAST, I_NAME, OL_QUANTITY, percentage): " +
                            "(%d, %d, %d, %d, %s, %s, %s, %s, %s, %d, %.2f)\n",
                    this.d_id, this.w_id, this.l, ol.ol_o_id, oIdToOEntryD.get(ol.ol_o_id),
                    c.c_first, c.c_middle, c.c_last, iIdToItemName.get(ol.ol_i_id.toString()),
                    ol.ol_quantity, itemIdOrderCounter.get(ol.ol_i_id) / this.l
            );
        }
    }
}
