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
package com.island.servlet;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.island.dao.PersonDao;
import com.island.entity.Person;
import com.island.entity.RoleRelationship;


/**
 * Servlet implementation class MaintainPersonServlet
 */
public class MaintainPersonServlet extends AbstractServlet {
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		ApplicationContext context = new FileSystemXmlApplicationContext("classpath:applicationContext.xml");
    	PersonDao personDao = (PersonDao)context.getBean("personDao");    	
		Long id = new Long(request.getParameter("id"));
		Person person = null;
		try {
			person = personDao.queryPerson(id);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		request.setAttribute("person", person);
		request.setAttribute("roles", getRolesString(person));
		request.getRequestDispatcher("/person/MaintainPerson.jsp").forward(request, response);
	}
	
	private String getRolesString(Person person){
		StringBuffer rolesString = new StringBuffer();
		Set<RoleRelationship> set = person.getRoleRelationships();
        Iterator<RoleRelationship> it = set.iterator();
        while(it.hasNext())
        {
        	RoleRelationship aa = it.next();
        	if(it.hasNext()){
        		rolesString.append("'" + aa.getRoleName() + "' ,");
        	}else
        	{
        		rolesString.append("'" + aa.getRoleName() + "'");
        	}
        }		
        return rolesString.toString();
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		ApplicationContext context = new FileSystemXmlApplicationContext("classpath:applicationContext.xml");
    	PersonDao personDao = (PersonDao)context.getBean("personDao");

    	Long id = new Long(request.getParameter("id"));
    	String lastName = request.getParameter("lastName");
    	String firstName = request.getParameter("firstName");
    	String[] roleNames = request.getParameterValues("roleName");
    	
    	try {
			personDao.updatePerson(id, lastName, firstName, roleNames);
    	} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("maintain for person(id:" + id + ") failed", e);
		}
        //Forward to the jsp page for rendering
        request.getRequestDispatcher("ListPerson").forward(request, response);
	}

}
