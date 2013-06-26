package com.island.test;

import com.island.stub.CalculatorServiceStub;

public class Test {
	public static void main(String[] args) throws Exception  
    {
		CalculatorServiceStub stub = new CalculatorServiceStub();
		CalculatorServiceStub.Add gg = new CalculatorServiceStub.Add();
        gg.setValue1(5);
        gg.setValue2(6);
        System.out.println( stub.add(gg).get_return());
    } 
}
