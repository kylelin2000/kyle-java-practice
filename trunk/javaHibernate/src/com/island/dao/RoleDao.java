package com.island.dao;

import java.sql.SQLException;
import java.util.List;

public interface RoleDao {
	public abstract List<Long> getRoleIds(String[] roleNames) throws SQLException;
}
