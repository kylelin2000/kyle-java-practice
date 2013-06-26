package com.island.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.island.stub.CalculatorServiceStub;

public class CalculatorServlet extends HttpServlet {
	protected void processRequest(HttpServletRequest request, HttpServletResponse response)
		    throws ServletException, IOException {
		String value1 = request.getParameter("value1");
		String value2 = request.getParameter("value2");
		String operator = request.getParameter("operator");
		
		String result = "";
		try {
			CalculatorServiceStub stub = new CalculatorServiceStub();
			
			if ("add".equals(operator)) {
				CalculatorServiceStub.Add add = new CalculatorServiceStub.Add();
				add.setValue1(Integer.parseInt(value1));
				add.setValue2(Integer.parseInt(value2));
				result = value1 + " ¥[ " + value2 + " = " + stub.add(add).get_return();
			} else if ("subtract".equals(operator)) {
				CalculatorServiceStub.Subtract subtract = new CalculatorServiceStub.Subtract();
				subtract.setValue1(Integer.parseInt(value1));
				subtract.setValue2(Integer.parseInt(value2));
				result = value1 + " ´î " + value2 + " = " + stub.subtract(subtract).get_return();
			} else if ("multiply".equals(operator)) {
				CalculatorServiceStub.Multiply multiply = new CalculatorServiceStub.Multiply();
				multiply.setValue1(Integer.parseInt(value1));
				multiply.setValue2(Integer.parseInt(value2));
				result = value1 + " ­¼ " + value2 + " = " + stub.multiply(multiply).get_return();
			} else if ("divide".equals(operator)) {
				CalculatorServiceStub.Divide divide = new CalculatorServiceStub.Divide();
				divide.setValue1(Integer.parseInt(value1));
				divide.setValue2(Integer.parseInt(value2));
				result = value1 + " °£ " + value2 + " = " + stub.divide(divide).get_return();
			}
			
			request.setAttribute("result", result);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		request.getRequestDispatcher("/").forward(request, response);
	}
	
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		processRequest(request, response);
	}

	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		processRequest(request, response);
	}
}
