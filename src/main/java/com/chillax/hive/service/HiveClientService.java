package com.chillax.hive.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Resource;

import org.springframework.data.hadoop.hive.HiveTemplate;
import org.springframework.stereotype.Service;

import com.chillax.hive.entry.Admin;

@Service("hiveService")
public class HiveClientService {

	@Resource(name = "hiveTemplate")
	private HiveTemplate hiveTemplate;

	public <T> List<T> query(String sql,Class<T> clazz) {
		List<String> result = hiveTemplate.query(sql);
		List<T> ts = new ArrayList<T>();
		System.out.println(result);
	/*	Iterator<String> it = result.iterator();
		while (it.hasNext()) {
			String current = it.next();
			String[] arr = current.split("\t");
		}*/
		return ts;
	}

}
