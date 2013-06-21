package com.island.dao.impl;

import org.hibernate.Query;
import org.hibernate.Session;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import org.hibernate.Transaction;

import java.util.Iterator;


import javax.sql.DataSource;

import com.island.dao.PersonDao;
import com.island.dao.RoleRelationshipDao;
import com.island.entity.Person;
import com.island.entity.RoleRelationship;

public class PersonDaoImpl implements PersonDao {

	private DataSource dataSource;
    
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
    
    @Override
    public void updatePerson(Long id, String lastName, String firstName, String[] roleName)  throws SQLException {
		try {
			Session session = HibernateUtil.getSessionFactory().openSession();
	    	Transaction tx = session.beginTransaction();   	
	        session.beginTransaction();  
	        Person t = (Person) session.get(Person.class,id);
	        t.setLastName(lastName);
	        t.setFirstName(firstName);
	        Set<RoleRelationship> scs = t.getRoleRelationships();
	        scs.clear();
	    	for(int i = 0;i < roleName.length;i++)
			{
				RoleRelationship user = new RoleRelationship(); 
		        if(roleName[i].equals("ADMIN")){
		        	user.setPersonRoleId(new Long(1));
		        	user.setRoleName(roleName[i]);		        	
		        }
		        else{
		        	user.setPersonRoleId(new Long(2));
		        	user.setRoleName(roleName[i]);		        	
		        }		
		        scs.add(user);
			}
	    	t.setRoleRelationships(scs);
	        session.update(t); 
	    	tx.commit();  
	        session.close();  
		}  finally {
		}
	}
    
    @Override
    public Person queryPerson(Long id) throws SQLException {

		Person person = null;		
		try {
			Session session = HibernateUtil.getSessionFactory().openSession();
	    	Transaction tx = session.beginTransaction();
			Query query = session.createQuery("from Person where id like ?");
	        query.setParameter(0,id);
	        
	        Iterator user = query.list().iterator();
	        while(user.hasNext()) {
	            person = (Person) user.next(); 	          
	        }
		}finally {
		}
		return (person == null) ? new Person() : person;
    }
 	@Override
	public void insertPerson(String firstName, String lastName ,String[] roleName) throws SQLException {
		try {
			Set<RoleRelationship> scs = new HashSet();
			for(int i = 0;i < roleName.length;i++)
			{
				RoleRelationship user = new RoleRelationship(); 
		        if(roleName[i].equals("ADMIN"))
		        {
		        	user.setPersonRoleId(new Long(1));
		        	user.setRoleName(roleName[i]);		        	
		        }
		        else{
		        	user.setPersonRoleId(new Long(2));
		        	user.setRoleName(roleName[i]);		        	
		        }		
		        scs.add(user);
			}			
	        Person user = new Person();
	        user.setLastName(lastName);
	        user.setFirstName(firstName);
	        user.setRoleRelationships(scs);	
	        Session session = HibernateUtil.getSessionFactory().openSession(); 
	        Transaction tx= session.beginTransaction(); 
	        session.save(user);
	        tx.commit(); 
	        session.close(); 
	        
		} finally {
		}
	}
	
	@Override
	public Long getNewPersonId() throws SQLException {
		Long newPersonId = null;		
		try {
			Session session = HibernateUtil.getSessionFactory().openSession();
	        Query query = session.createQuery("SELECT max(id) FROM Person");
	        Iterator users = query.list().iterator();
	        while(users.hasNext()) {
	        	newPersonId = (Long)users.next();
	        }
	        session.close();
		} finally {
		}
    	return newPersonId;
	}
	
	@Override
	public void deletePerson(Long id) throws SQLException {		
		try {
			Session session = HibernateUtil.getSessionFactory().openSession(); 
			Transaction tx = session.beginTransaction();
			Person person1 = (Person) session.load(Person.class,id);			
	    	session.delete(person1);
			tx.commit();
			session.close();
		} finally {
		}
	}
	
	@Override
    public Set<Person> queryPersons() throws SQLException {
		Set<Person> persons = new LinkedHashSet<Person>();
		Person person = null;
		try {
			 Session session = HibernateUtil.getSessionFactory().openSession(); 
		        Transaction tx = session.beginTransaction();
		        Query query = session.createQuery("from Person");
		        Iterator users = query.list().iterator();
		        while(users.hasNext()) {
		        	Person user = (Person) users.next(); 
		        	persons.add(user);		            
		        }
		} finally {
		}		
		return persons;
	}

	private RoleRelationshipDao generateCorrespondingRoles(Person person) {
		// TODO Auto-generated method stub
		return null;
	}
}
