<%@include file="/commons/decelerations.jsp" %>
<c:set var="submitButtonName"><c:choose>
	<c:when test='${action eq "CreatePerson"}'>Create Person</c:when>
	<c:otherwise>Maintain Person</c:otherwise>
</c:choose></c:set>

<form action="${action}" method="post" id="form">
   <table>
   	   <c:if test="${!(action eq 'CreatePerson')}">
   	   	<tr><td>Id</td><td><input type="text" id = "id" name="id" value="${person.id}" readonly="readonly"/></td></tr>
   	   </c:if>
       <tr><td>FirstName</td><td><input type="text" id = "firstName" name="firstName" value="${person.firstName}"/></td></tr>
       <tr><td>LastName</td><td><input type="text" id = "lastName" name="lastName" value="${person.lastName}"/></td></tr>
       <tr><td>Role</td>
       <td>
	       <label for="admin">Administrator</label><input type="checkbox" id="admin" name="roleName" value="ADMIN"/>
	       <label for="member">Member</label><input type="checkbox" id="member" name="roleName" value="MEMBER"/>
       </td></tr>
       <tr>
       	<td><input type="submit" value="${submitButtonName}"/></td>
       	<td><button type="reset">Reset</button></td>
       </tr>
   </table>
</form>
