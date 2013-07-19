
package com.island.practice.servlet;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


/**
 * Servlet implementation class DeletePersonServlet
 */
public class DeletePersonServlet extends AbstractServlet {
	
	/** Processes requests for both HTTP <code>GET</code> and <code>POST</code> methods.
     * @param request servlet request
     * @param response servlet response
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    	
    	String personId = request.getParameter("personId");
    	
    	Connection connection = null;
    	PreparedStatement statement = null;
    	
    	try {
			connection = this.getConnection();
	    	connection.setAutoCommit(false);
	    	statement = connection.prepareStatement(
	    			"DELETE " +
	    			"FROM PERSON " +
	    			"WHERE PERSON_ID = ? ");
	    	statement.setString(1, personId);
	    	statement.execute();
    	} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("delete person failed", e);
		} finally {
			closeStatement(statement);
			closeConnection(connection);
		}
        //Forward to the jsp page for rendering
        request.getRequestDispatcher("/person/ListPerson").forward(request, response);
    }
       
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		this.processRequest(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		this.processRequest(request, response);
	}

}
