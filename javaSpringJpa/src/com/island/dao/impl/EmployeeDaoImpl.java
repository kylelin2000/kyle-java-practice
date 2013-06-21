package com.island.dao.impl;

import java.sql.SQLException;
import java.util.List;

import javax.persistence.EntityManagerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.orm.jpa.support.JpaDaoSupport;
import org.springframework.stereotype.Repository;

import com.island.dao.EmployeeDao;
import com.island.entity.Employee;

@Repository("employeeDao")
public class EmployeeDaoImpl extends JpaDaoSupport implements EmployeeDao {
	@Autowired
	@Required
	public void setJpaEntityManagerFactory(@Qualifier("entityManagerFactory") EntityManagerFactory entityManagerFactory) {
		super.setEntityManagerFactory(entityManagerFactory);
	}
    
    @Override
    public void updateEmployee(Employee employee)  throws SQLException {
    	this.getJpaTemplate().merge(employee);
    	this.getJpaTemplate().flush();
	}
    
    @Override
    public Employee findById(Long id) throws SQLException {
		return this.getJpaTemplate().find(Employee.class, id);
    }
	
	@Override
	public void insertEmployee(Employee employee) throws SQLException {
		this.getJpaTemplate().persist(employee);
		this.getJpaTemplate().flush();
	}
	
	@Override
	public void deleteEmployee(Long id) throws SQLException {
		Employee employee = this.findById(id);
		if (employee != null) {
			this.getJpaTemplate().remove(employee);
			this.getJpaTemplate().flush();
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
    public List<Employee> queryEmployees() throws SQLException {
		return this.getJpaTemplate().find("from Employee order by id");
	}
}
