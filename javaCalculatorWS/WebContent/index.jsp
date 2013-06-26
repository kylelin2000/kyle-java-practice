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
	<a href="axis2-web/index.jsp" target="blank">查看有哪些Web Service</a>
	<form action="${pageContext.request.contextPath}/servlet/CalculatorServlet">
		請輸入兩個整數進行運算：
		<input type="text" id="value1" name="value1">
		<select id="operator" name="operator">
			<option value="add">加</option>
			<option value="subtract">減</option>
			<option value="multiply">乘</option>
			<option value="divide">除</option>
		</select>
		<input type="text" id="value2" name="value2">
		<input type="submit" value="送出">
		<br>
		結果：${result}
	</form>
</body>
</html>