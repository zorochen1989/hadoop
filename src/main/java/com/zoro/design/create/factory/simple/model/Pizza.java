package com.zoro.design.create.factory.simple.model;

/**
 * 披萨的抽象类
 */
public abstract class Pizza {

    /**
     * 披萨名称
     */
    private String name;

    public Pizza() {
    }

    public Pizza(String name) {
        this.name = name;
    }

    /**
     * 准备工作
     */
    public abstract Pizza prepare();

    /**
     * 烤
     */
    public Pizza bake() {
        System.out.println(this.hashCode()+"正在烤。。。。");
        return this;
    }

    /**
     * 切
     */
    public Pizza cut() {
        System.out.println(this.hashCode()+"正在切。。。。");
        return this;
    }

    /**
     * 打包
     */
    public Pizza box() {
        System.out.println(this.hashCode()+"正在打包。。。。");
        return this;
    }

}
