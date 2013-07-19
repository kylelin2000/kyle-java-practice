
package com.island.practice.entity;

import java.util.ArrayList;
import java.util.List;



/**
 * 
 * A Person model.
 * @author Kyle
 *
 */
public class Person {

    private String personId;

    private String lastName;

    private String firstName;
    
    private List<PersonRoleRelation> personRoleRelation;
    

    /**
     * Creates a new instance of Person
     */
    public Person() {
    }


	/**
	 * @return the id
	 */
	public String getPersonId() {
		return personId;
	}


	/**
	 * @param id the id to set
	 */
	public void setPersonId(String personId) {
		this.personId = personId;
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


	public List<PersonRoleRelation> getPersonRoleRelation() {
		return personRoleRelation;
	}


	public void setPersonRoleRelation(List<PersonRoleRelation> personRoleRelation) {
		this.personRoleRelation = personRoleRelation;
	}
    
    /**
     * A convenient method for getting Roles for this Person.
     * @return
     */
    public List<Role> getRoles(){
    	List<Role> roles = new ArrayList<Role>();
    	for(PersonRoleRelation roleRelationship : this.getPersonRoleRelation()){
    		roles.add(roleRelationship.getRole());
    	}
    	return roles;
    }
    
}
