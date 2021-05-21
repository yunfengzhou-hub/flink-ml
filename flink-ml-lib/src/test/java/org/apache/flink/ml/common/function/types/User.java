package org.apache.flink.ml.common.function.types;

public class User {
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
