package com.zoro.design.create.factory.simple.model;

/**
 * 苹果披萨
 */
public class ApplePizza extends Pizza {

    public ApplePizza() {
    }

    public ApplePizza(String name) {
        super(name);
    }

    @Override
    public ApplePizza prepare() {
        System.out.println("准备苹果披萨食材。。。");
        return this;
    }

}
