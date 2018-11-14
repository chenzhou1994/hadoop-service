package com.jeninfo.hadoopservice.lambda.method;

/**
 * @author chenzhou
 * @date 2018/11/13 15:39
 * @description
 */
@FunctionalInterface
public interface MyPredicate<T> {
    public boolean test(T t);
}
