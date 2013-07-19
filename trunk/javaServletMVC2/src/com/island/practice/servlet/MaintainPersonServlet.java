
package com.island.practice.servlet;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.island.practice.entity.Person;
import com.island.practice.entity.Role;
import com.island.practice.entity.PersonRoleRelation;


/**
 * Servlet implementation class MaintainPersonServlet
 */
public class MaintainPersonServlet extends AbstractServlet {
	

	private static final String SQL_UPDATE_PERSON = "UPDATE PERSON p " +
		    			"SET p.LAST_NAME = ?, p.FIRST_NAME = ? WHERE p.PERSON_ID = ? ";
	private static final String SQL_QUERY_PERSON = 
		"SELECT p.*, r.* FROM PERSON_ROLE_RELATION pr JOIN ROLE r ON pr.ROLE_ID = r.ROLE_ID " + 
	"RIGHT JOIN PERSON p ON p.PERSON_ID = pr.PERSON_ID WHERE p.PERSON_ID = ? ";
	
	private static final String SQL_DELETE_PERSON_ROLES =
		"DELETE FROM PERSON_ROLE_RELATION WHERE PERSON_ID = ?";
	
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String personId = request.getParameter("personId");
		
		Connection connection = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		Person person = null;
		List<PersonRoleRelation> roleRelationships = null;
		
		try {
			connection = this.getConnection();
		
			statement = connection.prepareStatement(SQL_QUERY_PERSON);
			statement.setString(1, personId);
			resultSet = statement.executeQuery();
			
			if(resultSet.next()){
				person = new Person();
				person.setPersonId(resultSet.getString("PERSON_ID"));
				person.setFirstName(resultSet.getString("FIRST_NAME"));
				person.setLastName(resultSet.getString("LAST_NAME"));
				
				roleRelationships = new ArrayList<PersonRoleRelation>();
				
				roleRelationships.add(generateRoleRelationship(resultSet));
				while(resultSet.next()){
					roleRelationships.add(generateRoleRelationship(resultSet));
				}
				
				person.setPersonRoleRelation(roleRelationships);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("Maintain person(personId:" + personId + ")", e);
		}
		
		request.setAttribute("person", person);
		request.setAttribute("roles", getRolesString(person));
		request.getRequestDispatcher("/person/MaintainPerson.jsp").forward(request, response);
	}
	
	private String getRolesString(Person person){
		StringBuffer rolesString = new StringBuffer();
		List<Role> roles = person.getRoles();
		Role role = null;
		for(int a = 0; a < roles.size(); a++){
			role = roles.get(a);
			if(a == 0){
				rolesString.append("'" + role.getRoleName() + "'");
			} else{
				rolesString.append(", '" + role.getRoleName() + "'");
			}
		}
		return rolesString.toString();
	}

	private PersonRoleRelation generateRoleRelationship(ResultSet resultSet) throws SQLException {
		PersonRoleRelation roleRelationship;
		Role role;
		role = new Role();
		role.setRoleId(resultSet.getString("ROLE_ID"));
		role.setRoleName(resultSet.getString("ROLE_NAME"));
		roleRelationship = new PersonRoleRelation();
		roleRelationship.setRole(role);
		
		return roleRelationship;
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	String personId = request.getParameter("personId");
    	String lastName = request.getParameter("lastName");
    	String firstName = request.getParameter("firstName");
    	String[] roleIds = request.getParameterValues("roleName");
    	
    	Connection connection = null;
    	PreparedStatement statement = null;
    	
    	try {
			connection = this.getConnection();
	    	connection.setAutoCommit(false);
	    	
	    	//1. update person
	    	statement = connection.prepareStatement(SQL_UPDATE_PERSON);
	    	
	    	statement.setString(1, lastName);
	    	statement.setString(2, firstName);
	    	statement.setString(3, personId);
	    	statement.execute();
	    	
	    	//2. delete roles
	    	statement = connection.prepareStatement(SQL_DELETE_PERSON_ROLES);
	    	statement.setString(1, personId);
	    	statement.execute();
	    	
	    	//3. re-insert selected roles
	    	this.insertPersonRolesRelationship(personId, roleIds, connection);
	    	
    	} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("maintain for person(personId:" + personId + ") failed", e);
		} finally {
			closeStatement(statement);
			closeConnection(connection);
		}
        //Forward to the jsp page for rendering
        request.getRequestDispatcher("ListPerson").forward(request, response);
	}

}
