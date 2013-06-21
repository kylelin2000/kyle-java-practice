package com.island.dao;

import java.sql.SQLException;
import java.util.Set;

import com.island.entity.Person;

public interface PersonDao {
	public abstract void updatePerson(Long id, String lastName, String firstName, String[] roleName)  throws SQLException;
	public abstract Person queryPerson(Long id) throws SQLException;
	public abstract void insertPerson(String firstName, String lastName , String[] roleName) throws SQLException;
	public abstract Long getNewPersonId() throws SQLException;
	public abstract void deletePerson(Long id) throws SQLException;
	public abstract Set<Person> queryPersons() throws SQLException;
}
