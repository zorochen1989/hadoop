package com.zoro.design.create.factory.simple.model;

public class MeatPizza extends Pizza {

    public MeatPizza() {
    }

    public MeatPizza(String name) {
        super(name);
    }

    @Override
    public Pizza prepare() {
        System.out.println("准备纯肉披萨食材。。。");
        return this;
    }
}
