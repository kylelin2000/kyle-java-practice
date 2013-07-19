
<%@include file="/commons/decelerations.jsp" %> 

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>List Of Persons</title>
    </head>
    <body>

    <h1>List of Persons currently in Database</h1>
<table id="personListTable" border="3">
<tr>
    <th>Person ID</th>
    <th>First Name</th>
    <th>Last Name</th>
    <th>Role Id</th>
    <th>Do Delete</th>
</tr>
<c:forEach var="person" begin="0" items="${requestScope.personList}">
<tr>
    <td><a href="MaintainPerson?personId=${person.personId}">${person.personId}</a>&nbsp;&nbsp;</td> 
    <td>${person.firstName}&nbsp;&nbsp;</td> 
    <td>${person.lastName}&nbsp;&nbsp;</td>
    <td><c:forEach var="role" items="${person.roles}" varStatus="roleStatus">
    	<c:if test="${!roleStatus.first}">,</c:if>
    	${role.roleId}
    </c:forEach></td>
    <td><a href="DeletePerson?personId=${person.personId}">Delete</a></td>
</tr> 
</c:forEach>

</table>
<a href="CreatePerson.jsp"><strong>Create a Person Record</strong></a>
</body>
</html>
