package com.zoro.design.create.singleton;

/**
 * 懒汉式单例模式
 */
public class SingletonType02 {

    public static void main(String[] args) {
        Singleton2 singleton = Singleton2.getInstance();
        Singleton2 singleton2 = Singleton2.getInstance();
        System.out.println(singleton == singleton2);

    }

    /**
     * 线程不安全的懒汉式
     * 多线程不能使用！
     */
    private static class Singleton {

        private Singleton() {
        }

        private static Singleton singleton;

        public static Singleton getInstance() {
            if (singleton == null) {
                singleton = new Singleton();
            }
            return singleton;
        }
    }

    /**
     * 线程安全的懒汉式
     * 不推荐使用，效率太低。
     */
    private static class Singleton2 {

        private Singleton2() {
        }

        private static Singleton2 singleton;

        public static synchronized Singleton2 getInstance() {
            if (singleton == null) {
                singleton = new Singleton2();
            }
            return singleton;
        }
    }
}
