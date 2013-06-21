
<%@include file="/commons/decelerations.jsp" %>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
   "http://www.w3.org/TR/html4/loose.dtd">
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>Create a Employee Record</title>
    </head>
    <body>

    <h1>Maintain a Employee record</h1>
    <c:set var="action" value="MaintainEmployee"></c:set>
    <%@include file="/commons/employeeInfo.jsp" %>
<a href="ListEmployee"><strong>Go to List of employees</strong></a>
</body>
</html>
