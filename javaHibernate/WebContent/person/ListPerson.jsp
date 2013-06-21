<%@include file="/commons/decelerations.jsp" %> 

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
   "http://www.w3.org/TR/html4/loose.dtd">
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>List Of Persons</title>
    </head>
    <body>

    <h1>List of Persons currently in Database</h1>
<table id="personListTable" border="3">
<tr>
    <th>ID</th>
    <th>FirstName</th>
    <th>LastName</th>
    <th>RoleName(s)</th>
    <th>Do Delete</th>
</tr>
<c:forEach var="person" begin="0" items="${requestScope.personList}">
<tr>
    <td><a href="MaintainPerson?id=${person.id}">${person.id}</a>&nbsp;&nbsp;</td> 
    <td>${person.firstName}&nbsp;&nbsp;</td> 
    <td>${person.lastName}&nbsp;&nbsp;</td>
    <td><c:forEach var="role" items="${person.roleRelationships}" varStatus="roleStatus">
    	<c:if test="${!roleStatus.first}">,</c:if>
    	<c:choose>
	    	<c:when test="${role.roleName eq 'ADMIN'}">Administrator</c:when>
	    	<c:when test="${role.roleName eq 'MEMBER'}">Member</c:when>
	    </c:choose>
    </c:forEach></td>
    <td><a href="DeletePerson?id=${person.id}">Delete</a></td>
</tr> 
</c:forEach>

</table>
<a href="CreatePerson.jsp"><strong>Create a Person Record</strong></a>
</body>
</html>
