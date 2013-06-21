package com.island.dao;

import java.sql.SQLException;
import java.util.List;

import org.springframework.transaction.annotation.Transactional;

import com.island.entity.Employee;

@Transactional
public interface EmployeeDao {
	public abstract void updateEmployee(Employee employee) throws SQLException;
	public abstract Employee findById(Long id) throws SQLException;
	public abstract void insertEmployee(Employee employee) throws SQLException;
	public abstract void deleteEmployee(Long id) throws SQLException;
	public abstract List<Employee> queryEmployees() throws SQLException;
}
