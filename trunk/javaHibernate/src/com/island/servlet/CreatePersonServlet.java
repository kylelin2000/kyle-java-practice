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
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.island.dao.PersonDao;
import com.island.dao.RoleDao;
import com.island.dao.RoleRelationshipDao;


/**
 * The sevelet class to insert Person into database
 */
public class CreatePersonServlet extends AbstractServlet {
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    	ApplicationContext context = new FileSystemXmlApplicationContext("classpath:applicationContext.xml");
    	PersonDao personDao = (PersonDao)context.getBean("personDao");
    	
        String firstName 	= request.getParameter("firstName");
        String lastName 	= request.getParameter("lastName");
        String[] roleNames 	= request.getParameterValues("roleName");

        try {
	    	personDao.insertPerson(firstName, lastName ,roleNames);
	    	} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("insert person failed", e);
		}
    	
    	request.getRequestDispatcher("/person/ListPerson").forward(request, response);
    	
    }
	
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
        processRequest(request, response);
    }
    
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
        processRequest(request, response);
    }
}
