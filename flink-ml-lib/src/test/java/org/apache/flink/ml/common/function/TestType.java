/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.common.function;

class TestType {
    public static class Order {
        public Long user;
        public String product;
        public int amount;

        public Order() {
            this.user = 1L;
            this.product = "product";
            this.amount = 1;
        }

        public Order(Long user, String product, int amount) {
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
            return (user.hashCode() * 31 + product.hashCode()) * 31 + amount;
        }

        @Override
        public boolean equals(Object obj) {
            if(!(obj instanceof Order)) return false;
            Order order = (Order)obj;

            return this.user.equals(order.user) && this.product.equals(order.product) && this.amount == order.amount;
        }
    }

    public static class ExpandedOrder extends Order {
        public Double price;

        public ExpandedOrder(){
            super();
            this.price = 0.0;
        }

        public ExpandedOrder(Long user, String product, int amount, Double price) {
            super(user, product, amount);
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
}
