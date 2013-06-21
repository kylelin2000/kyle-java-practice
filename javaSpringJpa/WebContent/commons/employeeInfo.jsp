<%@include file="/commons/decelerations.jsp" %>
<c:set var="submitButtonName"><c:choose>
	<c:when test='${action eq "CreateEmployee"}'>Create Employee</c:when>
	<c:otherwise>Maintain Employee</c:otherwise>
</c:choose></c:set>

<form action="${action}" method="post" id="form">
   <table>
   	   <c:if test="${!(action eq 'CreateEmployee')}">
   	   	<tr><td>Id</td><td><input type="text" id = "id" name="id" value="${employee.id}" readonly="readonly"/></td></tr>
   	   </c:if>
       <tr><td>FirstName</td><td><input type="text" id = "firstName" name="firstName" value="${employee.firstName}"/></td></tr>
       <tr><td>LastName</td><td><input type="text" id = "lastName" name="lastName" value="${employee.lastName}"/></td></tr>
       	<td><input type="submit" value="${submitButtonName}"/></td>
       	<td><button type="reset">Reset</button></td>
       </tr>
   </table>
</form>
