<%@include file="/commons/decelerations.jsp" %> 

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
   "http://www.w3.org/TR/html4/loose.dtd">
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>List Of Employees</title>
    </head>
    <body>

    <h1>List of Employees currently in Database</h1>
<table id="employeeListTable" border="3">
<tr>
    <th>ID</th>
    <th>FirstName</th>
    <th>LastName</th>
    <th>Do Delete</th>
</tr>
<c:forEach var="employee" begin="0" items="${requestScope.employeeList}">
<tr>
    <td><a href="MaintainEmployee?id=${employee.id}">${employee.id}</a>&nbsp;&nbsp;</td> 
    <td>${employee.firstName}&nbsp;&nbsp;</td> 
    <td>${employee.lastName}&nbsp;&nbsp;</td>
    <td><a href="DeleteEmployee?id=${employee.id}">Delete</a></td>
</tr> 
</c:forEach>
</table>
<a href="CreateEmployee.jsp"><strong>Create a Employee Record</strong></a>
</body>
</html>
