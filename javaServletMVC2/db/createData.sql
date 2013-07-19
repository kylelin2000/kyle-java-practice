--***********************************************************
--DDL for creating DB objects for training AP

--mysql -u root -p
--create database island;
--create user 'webuser'@'localhost' identified by 'webuser';
--grant all on island.* to 'webuser'@'localhost';
--flush privileges;

--***********************************************************
--drop PERSON table
DROP TABLE PERSON;
DROP TABLE PERSON_ROLE_RELATION;
DROP TABLE ROLE;

--create PERSON table
CREATE TABLE PERSON(
	PERSON_ID VARCHAR(20) PRIMARY KEY,
	LAST_NAME VARCHAR(50) NOT NULL,
	FIRST_NAME VARCHAR(50)  NOT NULL
);

CREATE TABLE ROLE (
	ROLE_ID VARCHAR(20) PRIMARY KEY,
	ROLE_NAME VARCHAR(50) NOT NULL
);

CREATE TABLE PERSON_ROLE_RELATION (
	PERSON_ID VARCHAR(20),
	ROLE_ID VARCHAR(20),
	
	PRIMARY KEY(PERSON_ID, ROLE_ID),
	CONSTRAINT PERSON_ID_FK FOREIGN KEY(PERSON_ID) REFERENCES PERSON(PERSON_ID) ON DELETE CASCADE,
	CONSTRAINT ROLE_ID_FK FOREIGN KEY(ROLE_ID) REFERENCES ROLE(ROLE_ID) ON DELETE CASCADE
);

--insert test data
--1. insert role records
INSERT INTO ROLE(ROLE_ID, ROLE_NAME)
VALUES('ADMIN', 'ADMIN');

INSERT INTO ROLE(ROLE_ID, ROLE_NAME)
VALUES('MEMBER', 'MEMBER');

--2. insert person records
INSERT INTO PERSON(PERSON_ID, LAST_NAME, FIRST_NAME)
VALUES('georgemike', 'George', 'Mike');

INSERT INTO PERSON(PERSON_ID, LAST_NAME, FIRST_NAME)
VALUES('maryanne', 'Mary', 'Anne');

--3. insert relationships between person and it's corresponding roles
INSERT INTO PERSON_ROLE_RELATION(PERSON_ID, ROLE_ID)
VALUES('georgemike', 'ADMIN');

INSERT INTO PERSON_ROLE_RELATION(PERSON_ID, ROLE_ID)
VALUES('maryanne', 'MEMBER');

