package com.island.servlet;

import java.io.IOException;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.island.dao.EmployeeDao;
import com.island.entity.Employee;


public class MaintainEmployeeServlet extends AbstractServlet {
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		ApplicationContext context = new FileSystemXmlApplicationContext("classpath:applicationContext.xml");
    	EmployeeDao employeeDao = (EmployeeDao)context.getBean("employeeDao");
    	
		Long id = new Long(request.getParameter("id"));
		Employee employee = null;
		try {
			employee = employeeDao.findById(id);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		request.setAttribute("employee", employee);
		request.getRequestDispatcher("/employee/MaintainEmployee.jsp").forward(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		ApplicationContext context = new FileSystemXmlApplicationContext("classpath:applicationContext.xml");
    	EmployeeDao employeeDao = (EmployeeDao)context.getBean("employeeDao");
    	
    	Long id = new Long(request.getParameter("id"));
    	String lastName = request.getParameter("lastName");
    	String firstName = request.getParameter("firstName");
    	
    	try {
    		Employee employee = new Employee();
    		employee.setId(id);
        	employee.setFirstName(firstName);
        	employee.setLastName(lastName);
    		employeeDao.updateEmployee(employee);
    	} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("maintain for employee(id:" + id + ") failed", e);
		}
        request.getRequestDispatcher("ListEmployee").forward(request, response);
	}
}
