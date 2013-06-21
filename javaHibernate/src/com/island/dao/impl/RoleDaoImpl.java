package com.island.dao.impl;

import org.hibernate.Session;
import java.util.Iterator;
import org.hibernate.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;


import com.island.dao.RoleDao;
import com.island.entity.Role;

public class RoleDaoImpl implements RoleDao {
	private DataSource dataSource;
    
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
    
    @Override
    public List<Long> getRoleIds(String[] roleNames) throws SQLException {
		List<Long> roleIds = new ArrayList<Long>();
		
		try {
			Session session = HibernateUtil.getSessionFactory().openSession(); 
			Query query = session.createQuery("from Role where name in (:idList)");
	        query.setParameterList("idList",roleNames);
	        Iterator users = query.list().iterator();
	        System.out.println("id \t name");
	        while(users.hasNext()) {
	        	Role user = (Role) users.next(); 
	            System.out.println(user.getId() +
	                    " \t " + user.getName()); 
	            roleIds.add(user.getId());
	        }
		} finally {
		}
		
		return roleIds;
	}
}
