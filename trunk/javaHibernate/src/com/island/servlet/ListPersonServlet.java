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
import java.util.LinkedHashSet;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.island.dao.PersonDao;
import com.island.entity.Person;

/**
 * The servlet class to list Persons from database
 */
public class ListPersonServlet extends AbstractServlet {
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    	WebApplicationContext wac = WebApplicationContextUtils.getWebApplicationContext(getServletContext());
    	PersonDao personDao = (PersonDao)wac.getBean("personDao");
    	Set<Person> persons = new LinkedHashSet<Person>();
    	
		try {			
			persons = personDao.queryPersons();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("get persons failed", e);
		}
    	
    	request.setAttribute("personList",persons);
        //Forward to the jsp page for rendering
        request.getRequestDispatcher("/person/ListPerson.jsp").forward(request, response);
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
        return "ListPerson servlet";
    }
}
