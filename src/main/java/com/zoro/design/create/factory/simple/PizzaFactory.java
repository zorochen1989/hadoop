package com.zoro.design.create.factory.simple;

import com.zoro.design.create.factory.simple.model.ApplePizza;
import com.zoro.design.create.factory.simple.model.MeatPizza;
import com.zoro.design.create.factory.simple.model.Pizza;

/**
 * 简单工厂类
 */
public class PizzaFactory {

    /**
     * 制作pizza
     *
     * @param type 披萨类型
     * @return 返回制作好的pizza
     */
    public Pizza createPizza(String type) {
        Pizza pizza = null;

        if ("apple".equalsIgnoreCase(type)) {
            pizza = new ApplePizza("苹果披萨");
        } else if ("meat".equalsIgnoreCase(type)) {
            pizza = new MeatPizza("纯肉披萨");
        }
        try {
            pizza.prepare().bake().cut().box();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("抱歉！没有您想要的pizza！");
        }
        return pizza;

    }
}
