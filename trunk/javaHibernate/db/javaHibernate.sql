
SET SQL_MODE="NO_AUTO_VALUE_ON_ZERO";

--
-- 資料表格式： `person`
--

CREATE TABLE IF NOT EXISTS `person` (
  `ID` bigint(20) NOT NULL AUTO_INCREMENT,
  `LAST_NAME` varchar(50) NOT NULL,
  `FIRST_NAME` varchar(50) NOT NULL,
  PRIMARY KEY (`ID`)
);

--
-- 列出以下資料庫的數據： `person`
--

INSERT INTO `person` (`ID`, `LAST_NAME`, `FIRST_NAME`) VALUES
(1, 'George', 'Mike'),
(2, 'Mary', 'Anne');

-- --------------------------------------------------------

--
-- 資料表格式： `person_role`
--

CREATE TABLE IF NOT EXISTS `person_role` (
  `ID` bigint(20) NOT NULL AUTO_INCREMENT,
  `ROLE_NAME` varchar(50) NOT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `ROLE_NAME` (`ROLE_NAME`),
  UNIQUE KEY `ROLE_NAME_2` (`ROLE_NAME`)
);

--
-- 列出以下資料庫的數據： `person_role`
--

INSERT INTO `person_role` (`ID`, `ROLE_NAME`) VALUES
(1, 'ADMIN'),
(2, 'MEMBER');

-- --------------------------------------------------------

--
-- 資料表格式： `person_roles`
--

CREATE TABLE IF NOT EXISTS `person_roles` (
  `PERSON_ROLES_ID` int(32) NOT NULL AUTO_INCREMENT,
  `PERSON_ID` bigint(20) DEFAULT NULL,
  `PERSON_ROLE_ID` bigint(20) DEFAULT NULL,
  `ROLE_NAME` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`PERSON_ROLES_ID`),
  KEY `PERSON_ROLES_PERSON_ROLE_PERSON_ROLE_ID_FK` (`PERSON_ROLE_ID`),
  KEY `PERSON_ID` (`PERSON_ID`)
);

--
-- 列出以下資料庫的數據： `person_roles`
--

INSERT INTO `person_roles` (`PERSON_ROLES_ID`, `PERSON_ID`, `PERSON_ROLE_ID`, `ROLE_NAME`) VALUES
(1, 1, 1, 'ADMIN'),
(2, 1, 2, 'MEMBER');

--
-- 備份資料表限制
--

--
-- 資料表限制 `person_roles`
--
ALTER TABLE `person_roles`
  ADD CONSTRAINT `person_roles_ibfk_1` FOREIGN KEY (`PERSON_ID`) REFERENCES `person` (`ID`) ON DELETE CASCADE ON UPDATE CASCADE;
