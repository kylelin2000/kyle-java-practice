package controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.web.servlet.mvc.Controller; 
import org.springframework.web.servlet.ModelAndView;

public class HelloController implements Controller {
    private String viewPage;
    
    public ModelAndView handleRequest(HttpServletRequest req, 
                                            HttpServletResponse res) 
                                                     throws Exception {
        String user = req.getParameter("user");
        return new ModelAndView(viewPage, "user", user);
    }
    
    public void setViewPage(String viewPage) {
        this.viewPage = viewPage;
    }
}
