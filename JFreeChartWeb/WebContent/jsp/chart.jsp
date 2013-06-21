<%@ page language="java" contentType="text/html; charset=BIG5" pageEncoding="BIG5"%>
<%@ page import="java.util.Date" %>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<HTML>     
    <HEAD>     
        <META http-equiv=Content-Type content="text/html; charset=UTF-8">     
    </HEAD>     
    <BODY>     
        <P ALIGN="CENTER">
        	<%=(new Date()) %>
            <img src="${requestScope.file1}" border=0 usemap="#imgMap">  
        </P>  
        <P ALIGN="CENTER">
            <img src="${requestScope.file2}" border=0 usemap="#imgMap">     
        </P>     
    </BODY>     
</HTML>