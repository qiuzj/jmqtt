package org.jmqtt.common.helper;

public class Pair<T, K> {
	/** 例如RequestProcessor */
    private T object1;
    /** 例如ExecutorService */
    private K object2;

    public Pair(T object1, K object2) {
        this.object1 = object1;
        this.object2 = object2;
    }

    public T getObject1() {
        return object1;
    }

    public void setObject1(T object1) {
        this.object1 = object1;
    }

    public K getObject2() {
        return object2;
    }

    public void setObject2(K object2) {
        this.object2 = object2;
    }
}
