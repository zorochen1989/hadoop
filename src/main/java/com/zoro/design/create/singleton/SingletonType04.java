package com.zoro.design.create.singleton;

/**
 * 静态内部类-单例模式
 * 推荐使用
 */
public class SingletonType04 {

    private SingletonType04() {
    }

    private static class Singleton {
        private static final SingletonType04 INSTANCE = new SingletonType04();
    }

    public static SingletonType04 getInstance() {
        return Singleton.INSTANCE;
    }

    public static void main(String[] args) {
        SingletonType04 instance = SingletonType04.getInstance();
        SingletonType04 instance2 = SingletonType04.getInstance();
        System.out.println(instance == instance2);
    }
}
