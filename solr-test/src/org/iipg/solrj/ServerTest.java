package org.iipg.solrj;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;

public class ServerTest {
	private SolrServer server;
	private HttpSolrServer httpServer;

	private static final String DEFAULT_URL = "http://localhost:8983/solr/";

	public void init() {
		httpServer = new HttpSolrServer(DEFAULT_URL);
		server = httpServer;

		httpServer.setSoTimeout(1000); // socket read timeout 
		httpServer.setConnectionTimeout(100); 
		httpServer.setDefaultMaxConnectionsPerHost(100); 
		httpServer.setMaxTotalConnections(100); 
		httpServer.setFollowRedirects(false); // defaults to false 
		// allowCompression defaults to false. 
		// Server side must support gzip or deflate for this to have any effect. 
		httpServer.setAllowCompression(true); 
		httpServer.setMaxRetries(1); // defaults to 0.  > 1 not recommended. 

		//sorlr J 目前使用二进制的格式作为默认的格式。对于solr1.2的用户通过显示的设置才能使用XML格式。
		httpServer.setParser(new XMLResponseParser());
	}

	public void destory() {
		server = null;
		httpServer = null;
		System.runFinalization();
		System.gc();
	}

	public final void fail(Object o) {
		System.out.println(o);
	}

	/**
	 * <b>function:</b> 测试是否创建server对象成功
	 */
	public void server() {
		fail(server);
		fail(httpServer);
	}

	/**
	 * <b>function:</b> 根据query参数查询索引
	 * @param query
	 */
	public void query(String query) {
		SolrParams params = new SolrQuery(query);

		try {
			QueryResponse response = server.query(params);

			SolrDocumentList list = response.getResults();
			for (int i = 0; i < list.size(); i++) {
				fail(list.get(i));
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		} 
	}

	public void addDoc() {
		//创建doc文档
		String owner = "刘长江";
		String name = "南京技胜科技有限公司";
		String taxType = "增值税";
		String catalog = "njgs_fzchgg";
		
		String dedup = name + "|" + taxType + "|" + catalog;
		
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("sys_dedup", dedup);
		doc.addField("owner", owner);
		doc.addField("name", name);
		doc.addField("cpNo", "320111686706867");
		doc.addField("address", "南京高新技术产业开发区星火路9号软件大厦B座510室");
		doc.addField("cardID", "610102196601043516");
		doc.addField("taxType", taxType);
		doc.addField("amount", "2543625");
		doc.addField("catalog", catalog);

		try {
			//添加一个doc文档
			UpdateResponse response = server.add(doc);
			fail(server.commit());//commit后才保存到索引库
			fail(response);
			fail("query time：" + response.getQTime());
			fail("Elapsed Time：" + response.getElapsedTime());
			fail("status：" + response.getStatus());
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ServerTest test = new ServerTest();
		test.init();
		test.addDoc();
		test.query("name:南京");
		test.destory();
	}

}
