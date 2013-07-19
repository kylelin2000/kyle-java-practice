
package com.island.practice.servlet;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.island.practice.entity.Person;
import com.island.practice.entity.Role;
import com.island.practice.entity.PersonRoleRelation;


/**
 * The servlet class to list Persons from database
 */
public class ListPersonServlet extends AbstractServlet {
    
    private static final String SQL_QUERY_PERSON_ROLE_RELATION = "SELECT t1.*, t2.ROLE_NAME FROM PERSON_ROLE_RELATION t1, ROLE t2 WHERE t1.ROLE_ID = t2.ROLE_ID AND t1.PERSON_ID = ? ";
	private static final String SQL_QUERY_PERSON = "SELECT * FROM PERSON ORDER BY PERSON_ID";

	/** Processes requests for both HTTP <code>GET</code> and <code>POST</code> methods.
     * @param request servlet request
     * @param response servlet response
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    	
    	Set<Person> persons = new LinkedHashSet<Person>();
    	Person person = null;
    	Connection connection = null;
    	PreparedStatement statement4Person = null;
    	ResultSet personResults = null;
    	
		try {
			connection = this.getConnection();
	    	statement4Person = connection.prepareStatement(SQL_QUERY_PERSON);
	    	personResults = statement4Person.executeQuery();
	    	
	    	while(personResults.next()){
	    		person = new Person();
	    		person.setPersonId(personResults.getString("PERSON_ID"));
	    		person.setLastName(personResults.getString("LAST_NAME"));
	    		person.setFirstName(personResults.getString("FIRST_NAME"));
	    		
	    		//find corresponding roles
	    		generateCorrespondingRoles(connection, person);
	    		
	    		persons.add(person);
	    	}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("get persons failed", e);
		} finally {
			closeStatement(statement4Person);
			closeConnection(connection);
		}
    	
    	request.setAttribute("personList",persons);
        //Forward to the jsp page for rendering
        request.getRequestDispatcher("/person/ListPerson.jsp").forward(request, response);
    }
    
    private void generateCorrespondingRoles(Connection connection, Person person) throws SQLException{
    	Role role = null;
    	PersonRoleRelation roleRelationship = null;
    	List<PersonRoleRelation> roleRelationships = null;
    	PreparedStatement statement4Role = null;
    	ResultSet roleResults = null;
    	
    	statement4Role = connection.prepareStatement(SQL_QUERY_PERSON_ROLE_RELATION);
		statement4Role.setString(1, person.getPersonId());
		roleResults = statement4Role.executeQuery();
		
		roleRelationships = new ArrayList<PersonRoleRelation>();
		while(roleResults.next()){
			role = new Role();
			role.setRoleId(roleResults.getString("ROLE_ID"));
			role.setRoleName(roleResults.getString("ROLE_NAME"));
			
			roleRelationship = new PersonRoleRelation();
			roleRelationship.setRole(role);
			roleRelationships.add(roleRelationship);
		}
		person.setPersonRoleRelation(roleRelationships);
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
    
    /** Returns a short description of the servlet.
     */
    public String getServletInfo() {
        return "ListPerson servlet";
    }
}
