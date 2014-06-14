package com.chillax.main;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import com.chillax.hive.service.HiveClientService;

public class ProgramEnter {
	
	public static void main(String[] args) {
		ClassPathXmlApplicationContext ac = new ClassPathXmlApplicationContext("classpath:spring-root.xml");
		ac.registerShutdownHook();
		
		HiveClientService hiveService = ac.getBean(HiveClientService.class);
		String sql = "select count(*) from stats_all";
		hiveService.query(sql,null);
		
//		JdbcTemplate
	}

}
