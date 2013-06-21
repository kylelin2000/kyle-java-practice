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


public class DeleteEmployeeServlet extends AbstractServlet {
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    	ApplicationContext context = new FileSystemXmlApplicationContext("classpath:applicationContext.xml");
    	EmployeeDao employeeDao = (EmployeeDao)context.getBean("employeeDao");
    	
    	Long id = new Long(request.getParameter("id"));
    	
    	try {
			employeeDao.deleteEmployee(id);
    	} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("delete employee failed", e);
		}
        //Forward to the jsp page for rendering
        request.getRequestDispatcher("/employee/ListEmployee").forward(request, response);
    }
       
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		this.processRequest(request, response);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		this.processRequest(request, response);
	}

}
