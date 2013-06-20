package com.island.ejb3;

import javax.ejb.Stateless;

/**
 * Session Bean implementation class HelloWorldBean
 */
@Stateless
public class HelloWorldBean implements HelloWorldBeanRemote {

    /**
     * Default constructor. 
     */
    public HelloWorldBean() {
    }

    public String sayHello() {
        return "Hello World !!!";
    }
}
