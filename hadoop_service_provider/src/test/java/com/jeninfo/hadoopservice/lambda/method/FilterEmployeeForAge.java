package com.jeninfo.hadoopservice.lambda.method;

import com.jeninfo.hadoopservice.model.Employee;

/**
 * @author chenzhou
 * @date 2018/11/13 15:41
 * @description
 */
public class FilterEmployeeForAge implements MyPredicate<Employee> {
    @Override
    public boolean test(Employee employee) {
        return employee.getAge() <= 35;
    }
}
