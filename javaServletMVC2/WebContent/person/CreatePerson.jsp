
<%@include file="/commons/decelerations.jsp" %>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
   "http://www.w3.org/TR/html4/loose.dtd">
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>Create a Person Record</title>
    </head>
    <body>

    <h1>Create a Person record</h1>
    <c:set var="action" value="CreatePerson"></c:set>
    <%@include file="/commons/personInfo.jsp" %>
<a href="ListPerson"><strong>Go to List of persons</strong></a>
</body>
</html>
