package com.zoro.design.create.factory.simple;

import com.zoro.design.create.factory.simple.model.Pizza;

public class PizzaOrder {

    private String pizzaType;

    public PizzaOrder() {
    }

    public PizzaOrder(String pizzaType) {
        this.pizzaType = pizzaType;
        PizzaFactory factory = new PizzaFactory();
        Pizza pizza = factory.createPizza(pizzaType);
        if (pizza != null) {
            System.out.println("披萨制作完成");
        } else {
            System.out.println("披萨订购失败");
        }
    }

    public static void main(String[] args) {
        PizzaOrder order = new PizzaOrder("meat");
        PizzaOrder order2 = new PizzaOrder("apple");
        PizzaOrder order3 = new PizzaOrder("apple1");
    }
}
