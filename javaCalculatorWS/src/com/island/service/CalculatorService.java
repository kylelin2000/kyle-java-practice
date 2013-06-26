package com.island.service;

public class CalculatorService implements ICalculatorService {
    public int add(int value1, int value2) {
        return value1 + value2;
    }
    
    public int subtract(int value1, int value2) {
        return value1 - value2;
    }
    
    public int multiply(int value1, int value2) {
        return value1 * value2;
    }
    
    public int divide(int value1, int value2) {
        return value1 / value2;
    }
}
