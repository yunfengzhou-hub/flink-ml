package org.apache.flink.ml.common.function.types;

import java.io.Serializable;

public class Order implements Serializable {
    public Long user;
    public String product;
    public Long amount;

    public Order() {
        this.user = 1L;
        this.product = "product";
        this.amount = 1L;
    }

    public Order(Long user, String product, Long amount) {
        this.user = user;
        this.product = product;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Order{"
                + "user="
                + user
                + ", product='"
                + product
                + '\''
                + ", amount="
                + amount
                + '}';
    }

    @Override
    public int hashCode() {
        return (user.hashCode() * 31 + product.hashCode()) * 31 + amount.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Order)) return false;
        Order order = (Order) obj;

        return this.user.equals(order.user) && this.product.equals(order.product) && this.amount == order.amount;
    }
}
