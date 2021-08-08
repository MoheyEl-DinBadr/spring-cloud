package com.mohey.springcloud.Logging;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

@Configuration
@Aspect
public class AOP {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Pointcut("within(org.springframework.kafka.listener..*)")
    public void definePackagePointcuts(){
        // empty method just to name the locations specified in the pointcut
    }

    @AfterThrowing(value = "definePackagePointcuts()", throwing = "ex")
    public void logAround(JoinPoint jp, Exception ex){
        //@After logs the code after it hits or compiles the method, @Before is after the event trigger but before the method execution
        //@Around you can control before and after the method trigger

        System.err.println("\n \n \n");
        System.err.println("************ After Throwing ************ \n "
                        + jp.getSignature().getDeclaringTypeName() +
                        ". " +
                jp.getSignature().getName() +  " () with arguments "+ Arrays.toString(jp.getArgs()) +" = {}");

        System.err.println(ex.getMessage());
        System.err.println("______________________________________________________ \n \n \n");

    }
}
