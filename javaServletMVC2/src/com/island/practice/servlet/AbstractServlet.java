/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.island.practice.servlet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServlet;

/**
 * Servlet implementation class AbstractServlet
 * This servlet stores common method for it's sub-classes. 
 */
public class AbstractServlet extends HttpServlet {
	
	private static final String SQL_QUERY_ROLES_ID = "SELECT ROLE_ID FROM ROLE WHERE ROLE_NAME IN ({0})";
	private static final String QUESTION_MARK = "?";
	
	private static final String SQL_INSERT_PERSON_ROLE_RELATION = 
		"INSERT INTO PERSON_ROLE_RELATION(PERSON_ID, ROLE_ID) " +
		"VALUES(?, ?); ";
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public AbstractServlet() {
        super();
    }
    
    protected final String getConnectionString(){
    	return this.getServletContext().getInitParameter("connectionString");
    }
    
    protected final String getUserName(){
    	return this.getServletContext().getInitParameter("userName");
    }
    
    protected final String getPassword(){
    	return this.getServletContext().getInitParameter("password");
    }
    
    protected final Connection getConnection() throws SQLException{
    	try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
    	return DriverManager.getConnection(this.getConnectionString(), this.getUserName(), this.getPassword());
    }
    
    protected final List<String> getRoleIds(Connection connection, String[] roleNames) throws SQLException {
		List<String> roleIds = new ArrayList<String>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		StringBuffer roleNameParams = new StringBuffer();
		String sql = null;
		
		try {
	    	for(int a = 0; a < roleNames.length; a++){
	    		if(a == 0){
	    			roleNameParams.append(QUESTION_MARK);
	    		} else{
	    			roleNameParams.append(", " + QUESTION_MARK);
	    		}
	    	}
	    	sql = MessageFormat.format(SQL_QUERY_ROLES_ID, roleNameParams.toString());
	    	statement = connection.prepareStatement(sql);
	    	for(int a = 0; a < roleNames.length; a++){ 
	    		statement.setString(a + 1, roleNames[a]);
	    	}
	    	resultSet = statement.executeQuery(); 
	    	while(resultSet.next()){
	    		roleIds.add(resultSet.getString("ROLE_ID"));
	    	}
		} finally {
			closeStatement(statement);
		}
		
		return roleIds;
	}

	protected final void insertPersonRolesRelationship(String personId, String[] roleIds, Connection connection) throws SQLException {
				String roleId = null;
				PreparedStatement statement = null;
				
				try{
					statement = connection.prepareStatement(SQL_INSERT_PERSON_ROLE_RELATION);
					
					for(int a = 0; a < roleIds.length; a++){
						roleId = roleIds[a];
						statement.setString(1, personId);
						statement.setString(2, roleId);
						statement.execute();
					}
				} finally {
					closeStatement(statement);
				}
			}

	protected final static void closeConnection(Connection connection){
    	closeConnection(connection, true);
    }
    
    protected final static void closeConnection(Connection connection, boolean setAutoCommit) {
		if(null != connection)
			try {
				connection.setAutoCommit(setAutoCommit);
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
				throw new RuntimeException("close connection failed", e);
			}
	}

	protected final static void closeStatement(PreparedStatement statement) {
		if(null != statement)
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
				throw new RuntimeException("close statement failed", e);
			}
	}

}
