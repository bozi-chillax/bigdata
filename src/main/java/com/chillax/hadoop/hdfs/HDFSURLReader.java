package com.chillax.hadoop.hdfs;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * 从hadoop文件中读取数据
 * @author chillax
 *
 */
public class HDFSURLReader {

	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}

	public static void main(String[] args) {
		InputStream in = null;
		try {
			String hdfsUrl = "hdfs://192.168.0.127:9000/user/test.txt";
			in = new URL(hdfsUrl).openStream();
			IOUtils.copyBytes(in, System.out, 1024, false);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		} finally {
			IOUtils.closeStream(in);
		}
	}

}
