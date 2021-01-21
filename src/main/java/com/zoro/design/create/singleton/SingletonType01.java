package com.zoro.design.create.singleton;

/**
 * 饿汉式单例模式：
 */
public class SingletonType01 {

    public static void main(String[] args) {
        Singleton2 instance1 = Singleton2.getInstance();
        Singleton2 instance2 = Singleton2.getInstance();
        System.out.println(instance1 == instance2);
    }

    /**
     * 静态常量饿汉式
     * 可以用，可能会造成内存浪费。
     */
    private static class Singleton {

        // 构造器私有化
        private Singleton() {
        }

        // 创建实例
        private static Singleton singleton = new Singleton();

        // 返回实例
        public static Singleton getInstance() {
            return singleton;
        }
    }

    /**
     * 静态代码块饿汉式
     * 可以用，可能会造成内存浪费。
     */
    private static class Singleton2 {

        private Singleton2() {
        }

        private static Singleton2 singleton2;

        static {
            singleton2 = new Singleton2();
        }

        // 返回实例
        public static Singleton2 getInstance() {
            return singleton2;
        }

    }
}


