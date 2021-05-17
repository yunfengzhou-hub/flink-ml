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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.sql.Timestamp;

class TestType {
    public static class Order implements Serializable {
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

    public static class ExpandedOrder {
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

    public static class User {
        public Long user;
        public User() {
            this.user = 1L;
        }

        public User(Long user) {
            this.user = user;
        }

        @Override
        public int hashCode() {
            return user.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof User)) return false;
            User order = (User) obj;

            return this.user.equals(order.user);
        }
    }

    public static class Meeting {
        public @DataTypeHint(value = "TIMESTAMP(3)", bridgedTo = java.sql.Timestamp.class) Timestamp time;
        public String event;

        public Meeting(){
            time = new Timestamp(1);
            event = "default meeting event";
        }

        public Meeting(int millis, String event){
            this.time = new Timestamp(millis);
            this.event = event;
        }

        @Override
        public int hashCode() {
            return time.hashCode() * 31 + event.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if(!(obj instanceof Meeting)){
                return false;
            }
            Meeting meeting = (Meeting) obj;
            return this.time.equals(meeting.time) && this.event.equals(meeting.event);
        }
    }

    public static void main(String[] args) {
        Order order = new Order();
        RowData rowData = (RowData) order;
        System.out.println(rowData);
    }
}
