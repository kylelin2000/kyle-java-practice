<%@ page language="java" contentType="text/html; charset=big5"
    pageEncoding="big5"%>
<%@ page
	import="java.net.URL,javax.xml.namespace.QName,javax.xml.ws.Service,com.island.service.CalculatorService"%>

<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=big5">
<title>Calculator</title>
</head>
<body>
	<a href="axis2-web/index.jsp" target="blank">�d�ݦ�����Web Service</a>
	<form action="${pageContext.request.contextPath}/servlet/CalculatorServlet">
		�п�J��Ӿ�ƶi��B��G
		<input type="text" id="value1" name="value1">
		<select id="operator" name="operator">
			<option value="add">�[</option>
			<option value="subtract">��</option>
			<option value="multiply">��</option>
			<option value="divide">��</option>
		</select>
		<input type="text" id="value2" name="value2">
		<input type="submit" value="�e�X">
		<br>
		���G�G${result}
	</form>
</body>
</html>