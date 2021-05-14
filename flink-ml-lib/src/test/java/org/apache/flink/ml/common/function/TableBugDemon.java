package org.apache.flink.ml.common.function;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TableBugDemon {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv =StreamTableEnvironment.create(env);
        DataStream<Order> input = env.fromElements(new Order());
        Table table = tEnv.fromDataStream(input);

        Table table1 = table.select(
                $("user").times(3).as("user"),
                $("product"),
                $("amount").times(2).as("amount")
        );
        Table table2 = table.unionAll(table1);// union也复现出了这个问题
        DataStream<Order> output = tEnv.toAppendStream(table2, Order.class);
        output.print();
        env.execute();
    }

    public static class Order {
        public Long user;
        public String product;
        public Long amount;

        public Order() {
            this(1L, "product", 1L);
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
}
