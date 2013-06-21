package com.island.dao.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;

import com.island.dao.RoleRelationshipDao;
import com.island.entity.Person;
import com.island.entity.Role;
import com.island.entity.RoleRelationship;

public class RoleRelationshipDaoImpl implements RoleRelationshipDao {
	private DataSource dataSource;
    
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
    
    @Override
	public void insertPersonRolesRelationship(String[] roleNames, Long newPersonId, List<Long> roleIds) throws SQLException {
		Long roleId = null;
		PreparedStatement statement = null;

		try {
		       for (int a = 0; a < roleIds.size(); a++) {
		    	    roleId = roleIds.get(a);
		        	
		    	    Map role_temp = new HashMap();
		    	    role_temp.put("personid", newPersonId);
		    	    role_temp.put("personroleid", roleId); 
		    	    role_temp.put("rolename", roleNames[a]);

		        	Session session = HibernateUtil.getSessionFactory().openSession(); 
		        	Transaction tx= session.beginTransaction(); 
		        	session.save("Personroles", role_temp);
		        	tx.commit(); 
		        	session.close();
		        	
				}

		       
		} finally {
		}
	}
    
    @Override
    public void deletePersonRoles(Long id)  throws SQLException {
		PreparedStatement statement = null;
		try {
			
			Session session = HibernateUtil.getSessionFactory().openSession(); 
			Transaction tx = session.beginTransaction();
			Query query = session.createQuery("delete Personroles where personid=?");
			query.setParameter(0, id);
			query.executeUpdate();
			tx.commit();
			session.close();
		} finally {
		}
    }
    
    @Override
    public void generateCorrespondingRoles(Person person) throws SQLException {
    	Role role = null;
    	RoleRelationship roleRelationship = null;
    	List<RoleRelationship> roleRelationships = null;
    	PreparedStatement statement4Role = null;
    	ResultSet roleResults = null;
    	
    	Session session = HibernateUtil.getSessionFactory().openSession(); 
    	Query query = session.createQuery("from Personroles where personid like ?");
        query.setParameter(0,person.getId());
        List userss = query.list(); 
        
        roleRelationships = new ArrayList<RoleRelationship>();
        for(int i = 0; i < userss.size(); i++) {
            Map user = (Map) userss.get(i);
            
    		role = new Role();
    		role.setId((Long) user.get("personroleid"));
    		role.setName((String) user.get("rolename"));
    		
    		roleRelationship = new RoleRelationship();
			//roleRelationship.setRole(role);
			roleRelationships.add(roleRelationship);
			
        }
    }
}
