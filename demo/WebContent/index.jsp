<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ page import="org.springframework.web.context.WebApplicationContext"%>
<%@ page
	import="org.springframework.web.context.support.WebApplicationContextUtils"%>
<%@ page import="net.butfly.demo.albacore.dao.UserDAO"%>
<%@ page import="net.butfly.demo.albacore.mapper.User"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Albacore Sample Page</title>
<%
	WebApplicationContext context = WebApplicationContextUtils.getWebApplicationContext(application);
	UserDAO dao = (UserDAO) context.getBean("userDAO");
	User[] users = dao.select(null);
%>
</head>
<body>
	<div style="margin: 2em; padding: 1em;">
		<h2>
			count of users: <span style="font-style: italic; color: red;"><%=dao.count(null)%></span>
		</h2>
		<%
			for (User u : users) {
		%>
		<h3>
			name of user with id[<span style="color: blue;"><%=u.getId()%></span>]:&nbsp;
			<span style="font-style: italic; color: red;"><%=dao.load(u.getId()).getName()%></span>
		</h3>
		<%
			}
		%>
	</div>
</body>
</html>