package com.island.servlet;

import java.io.IOException;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.island.dao.EmployeeDao;
import com.island.entity.Employee;

public class CreateEmployeeServlet extends AbstractServlet {
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    	ApplicationContext context = new FileSystemXmlApplicationContext("classpath:applicationContext.xml");
    	EmployeeDao employeeDao = (EmployeeDao)context.getBean("employeeDao");
    	
    	//Get the data from user's form
        String firstName 	= request.getParameter("firstName");
        String lastName 	= request.getParameter("lastName");
    	
        try {
        	Employee employee = new Employee();
        	employee.setFirstName(firstName);
        	employee.setLastName(lastName);
	    	employeeDao.insertEmployee(employee);
        } catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("insert employee failed", e);
		}
    	
    	request.getRequestDispatcher("/employee/ListEmployee").forward(request, response);
    	
    }
	
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
        processRequest(request, response);
    }
    
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
        processRequest(request, response);
    }
}
