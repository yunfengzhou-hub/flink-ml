package org.apache.flink.ml.common.function.types;

public class ExpandedOrder {
    public Double price;
    public Long user;
    public String product;
    public Long amount;

    public ExpandedOrder(){
        this(1L, "product", 1L, 0.0);
    }

    public ExpandedOrder(Long user, String product, Long amount, Double price) {
        this.user = user;
        this.product = product;
        this.amount = amount;
        this.price = price;
    }

    @Override
    public int hashCode() {
        return super.hashCode() * 31 + price.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof ExpandedOrder)) return false;
        ExpandedOrder order = (ExpandedOrder)obj;

        return super.equals(obj) && this.price.equals(order.price);
    }
}
