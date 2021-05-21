package org.apache.flink.ml.common.function.tomcat;

import java.io.Serializable;

public class MenuItem implements Serializable {
    public String name;
    public Double price;

    public MenuItem() {
        this("burger", 9.99);
    }

    public MenuItem(String name, Double price){
        this.name = name;
        this.price = price;
    }

    @Override
    public int hashCode() {
        return name.hashCode() * 31 + price.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof MenuItem))  return false;
        MenuItem item = (MenuItem)obj;
        return this.name.equals(item.name) && this.price.equals(item.price);
    }

    @Override
    public String toString() {
        String result = "";
        result += this.getClass().getSimpleName();
        result += "{";
        result += String.format("name=%s, ", name);
        result += String.format("price=%s", price);
        result += "}";
        return result;
    }
}