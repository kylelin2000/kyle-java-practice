package com.island.servlet;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.island.dao.EmployeeDao;
import com.island.entity.Employee;


public class ListEmployeeServlet extends AbstractServlet {
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    	WebApplicationContext wac = WebApplicationContextUtils.getWebApplicationContext(getServletContext());
    	EmployeeDao employeeDao = (EmployeeDao)wac.getBean("employeeDao");
    	
    	List<Employee> employees = new ArrayList<Employee>();
    	
		try {
			employees = employeeDao.queryEmployees();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("get employees failed", e);
		}
    	
    	request.setAttribute("employeeList",employees);
        //Forward to the jsp page for rendering
        request.getRequestDispatcher("/employee/ListEmployee.jsp").forward(request, response);
    }
    
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
        processRequest(request, response);
    }
    
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
        processRequest(request, response);
    }
    
    public String getServletInfo() {
        return "ListEmployee servlet";
    }
}
