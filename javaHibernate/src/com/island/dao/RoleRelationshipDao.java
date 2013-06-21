package com.island.dao;

import java.sql.SQLException;
import java.util.List;

import com.island.entity.Person;

public interface RoleRelationshipDao {
	public abstract void insertPersonRolesRelationship(String[] roleNames, Long newPersonId, List<Long> roleIds) throws SQLException;
	public abstract void deletePersonRoles(Long id)  throws SQLException;
	public abstract void generateCorrespondingRoles(Person person) throws SQLException;
}
