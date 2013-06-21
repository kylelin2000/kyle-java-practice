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
import java.io.Serializable;
/**
 * A class for solving N-N cardinality between Person and Role models.
 * @author takeshi.miao
 *
 */
public class RoleRelationship  implements Serializable{
	
	//private Role role;
	private Long personRolesId;
	private Long personId;
    private Long personRoleId;
    private String roleName;
    private Person person;
	    
	 
    public RoleRelationship() {}
    /**
	 * @return the personId
	 */
    public Long getPersonId() {
        return personId;
    }
    /**
	 * @param personId the personId to set
	 */
    public void setPersonId(Long personId) {
        this.personId = personId;
    }
    /**
	 * @return the roleId
	 */
    public Long getPersonRoleId() {
        return personRoleId;
    }
    /**
	 * @param roleId the roleId to set
	 */
    public void setPersonRoleId(Long personRoleId) {
        this.personRoleId = personRoleId;
    }
    /**
	 * @return the roleName
	 */
    public String getRoleName() {
		return roleName;
	}
    /**
	 * @param roleName the roleName to set
	 */
    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }
	public Person getPerson() {
		return person;
	}
	public void setPerson(Person person) {
		this.person = person;
	}
	
	 public boolean equals(Object other) {
	        if ( (this == other ) ) return true;
	   if ( (other == null ) ) return false;
	   if ( !(other instanceof RoleRelationship) ) return false;
	   RoleRelationship castOther = ( RoleRelationship ) other; 
	        
	   return ( (this.getPersonId()==castOther.getPersonId()) || ( this.getPersonId()!=null && castOther.getPersonId()!=null && this.getPersonId().equals(castOther.getPersonId()) ) )
	&& ( (this.getPersonRoleId()==castOther.getPersonRoleId()) || ( this.getPersonRoleId()!=null && castOther.getPersonRoleId()!=null && this.getPersonRoleId().equals(castOther.getPersonRoleId()) ) );
	}

	public int hashCode() {
	        int result = 17;
	        
	        result = 37 * result + ( getPersonId() == null ? 0 : this.getPersonId().hashCode() );
	        result = 37 * result + ( getPersonRoleId() == null ? 0 : this.getPersonRoleId().hashCode() );
	        return result;
	}
	public Long getPersonRolesId() {
		return personRolesId;
	}
	public void setPersonRolesId(Long personRolesId) {
		this.personRolesId = personRolesId;
	} 
	
	
}
