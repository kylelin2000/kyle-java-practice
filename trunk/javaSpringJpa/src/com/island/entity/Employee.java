package com.island.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

@Entity 
@Table(name = "EMPLOYEE")
@SequenceGenerator(name="EMPLOYEE_ID_SEQ",sequenceName="EMPLOYEE_ID_SEQ")  
public class Employee implements Serializable {
    private Long id;
    private String lastName;
    private String firstName;

    public Employee() {
    }

    @Id
    @GeneratedValue(strategy=GenerationType.SEQUENCE, generator="EMPLOYEE_ID_SEQ")
    @SequenceGenerator(name="EMPLOYEE_ID_SEQ", sequenceName="EMPLOYEE_ID_SEQ", allocationSize=1)
	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	@Column(name="LAST_NAME")
	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	@Column(name="FIRST_NAME")
	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
}
