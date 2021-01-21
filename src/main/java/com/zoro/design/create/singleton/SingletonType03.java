package com.zoro.design.create.singleton;

/**
 * 单例模式-双重检查
 * 推荐使用
 */
public class SingletonType03 {
    public static void main(String[] args) {
        Singleton singleton = Singleton.getInstance();
        Singleton singleton2 = Singleton.getInstance();
        System.out.println(singleton == singleton2);
    }

    private static class Singleton {

        private Singleton() {
        }

        private static volatile Singleton singleton;

        public static Singleton getInstance() {
            if (singleton == null) {
                synchronized (Singleton.class) {
                    if (singleton == null) {
                        singleton = new Singleton();
                    }
                }
            }

            return singleton;
        }
    }
}
