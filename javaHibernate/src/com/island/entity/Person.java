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

package com.island.entity;

import java.util.ArrayList;
import javax.persistence.*;
import java.util.List;
import java.util.Set;


/**
 * 
 * A Person model.
 * @author takeshi.miao
 *
 */
/**
 * @author takeshi.miao
 *
 */		
@Entity
@Table(name = "Person")
public class Person {
	
	
	
    //@Id @GeneratedValue(strategy = GenerationType.AUTO)
    //@Column(name = "ID") // 非必要，在欄位名稱與屬性名稱不同時使用
    private Long id;
    //@Column(name = "LAST_NAME") // 非必要，在欄位名稱與屬性名稱不同時使用
    private String lastName;
    //@Column(name = "FIRST_NAME") // 非必要，在欄位名稱與屬性名稱不同時使用
    private String firstName;
    
    private Set roleRelationships;
    
    

    /**
     * Creates a new instance of Person
     */
    public Person() {
    }


	/**
	 * @return the id
	 */
	public Long getId() {
		return id;
	}


	/**
	 * @param id the id to set
	 */
	public void setId(Long id) {
		this.id = id;
	}


	/**
	 * @return the lastName
	 */
	public String getLastName() {
		return lastName;
	}


	/**
	 * @param lastName the lastName to set
	 */
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}


	/**
	 * @return the firstName
	 */
	public String getFirstName() {
		return firstName;
	}


	/**
	 * @param firstName the firstName to set
	 */
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	/**
	 * @return the roleRelationships
	 */
	public Set getRoleRelationships() {
		return roleRelationships;
	}
	/**
	 * @param roleRelationships the roleRelationships to set
	 */
	public void setRoleRelationships(Set roleRelationships) {
		this.roleRelationships = roleRelationships;
	}
	public void addRoleRelationship(RoleRelationship roleRelationships) {
		this.roleRelationships.add(roleRelationships);
    }
 
    public void removeRoleRelationship(RoleRelationship roleRelationships) {
    	this.roleRelationships.remove(roleRelationships);
    }


	


	
    
}
