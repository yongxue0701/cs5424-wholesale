package edu.cs5424.datatype;

public class OrderedItem {
    private final int itemNumber;
    private final String itemName;
    private final int supplierWarehouse;
    private final int quantity;
    private final double orderAmount;
    private final int stockQuantity;

    public OrderedItem(
            int itemNumber,
            String itemName,
            int supplierWarehouse,
            int quantity,
            double orderAmount,
            int stockQuantity
    ) {
        this.itemNumber = itemNumber;
        this.itemName = itemName;
        this.supplierWarehouse = supplierWarehouse;
        this.quantity = quantity;
        this.orderAmount = orderAmount;
        this.stockQuantity = stockQuantity;
    }

    public int getItemNumber() {
        return itemNumber;
    }

    public String getItemName() {
        return itemName;
    }

    public int getSupplierWarehouse() {
        return supplierWarehouse;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getOrderAmount() {
        return orderAmount;
    }

    public int getStockQuantity() {
        return stockQuantity;
    }
}