
package com.island.practice.servlet;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


/**
 * The sevelet class to insert Person into database
 */
public class CreatePersonServlet extends AbstractServlet {
	
	private static final String SQL_INSERT_PERSON = 
		"INSERT INTO PERSON(PERSON_ID, LAST_NAME, FIRST_NAME) VALUES(?, ?, ?)";

	/** Processes requests for both HTTP <code>GET</code> and <code>POST</code> methods.
     * @param request servlet request
     * @param response servlet response
     * @throws IOException 
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    	//Get the data from user's form
    	String personId		= request.getParameter("personId");
        String firstName 	= request.getParameter("firstName");
        String lastName 	= request.getParameter("lastName");
        String[] roleIds 	= request.getParameterValues("roleName");
    	
        Connection connection = null;
        PreparedStatement statement = null;
        try {
			connection = this.getConnection();
			//HEAD: transaction boundary
	        connection.setAutoCommit(false);
	        //1. insert person
	    	insertPerson(personId, firstName, lastName, connection);
	    	
	    	//4. insert person_roles relationship
	    	insertPersonRolesRelationship(personId, roleIds, connection);
    	
        } catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("insert person failed", e);
		} finally {
			closeStatement(statement);
			//TAIL: transaction boundary
			closeConnection(connection);
		}
    	
    	request.getRequestDispatcher("/person/ListPerson").forward(request, response);
    	
    }

	private void insertPerson(String personId, String firstName, String lastName, Connection connection)
			throws SQLException {
		PreparedStatement statement = null;
		
		try {
			statement = connection.prepareStatement(SQL_INSERT_PERSON);
			statement.setString(1, personId);
			statement.setString(2, lastName);
			statement.setString(3, firstName);
			statement.execute();
		} finally {
			closeStatement(statement);
		}
	}
	
	// <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /** Handles the HTTP <code>GET</code> method.
     * @param request servlet request
     * @param response servlet response
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
        processRequest(request, response);
    }
    
    /** Handles the HTTP <code>POST</code> method.
     * @param request servlet request
     * @param response servlet response
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
        processRequest(request, response);
    }
}
