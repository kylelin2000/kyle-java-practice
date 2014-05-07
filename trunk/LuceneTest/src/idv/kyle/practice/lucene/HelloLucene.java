package idv.kyle.practice.lucene;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.UUID;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
/*
 *@author: ZhengHaibo  
 *web:     http://blog.csdn.net/nuptboyzhb
 *mail:    zhb931706659@126.com
 *2013-7-05  Nanjing,njupt,China
 */
public class HelloLucene {
	public void indexSimple(String indexPath) {
		try {
			// 1.创建Directory
			Directory dir = FSDirectory.open(new File(indexPath));//保存在硬盘上
			// 2.创建IndexWriter
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
			IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47,
					analyzer);
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);// 设置创建或追加模式
			IndexWriter writer = new IndexWriter(dir, iwc);
			indexing(writer);
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Index all text files under a directory.
	 * String indexPath = "index";//索引保存的路径
	 * String docsPath = "";//文档保存的路径（待索引）
	 */
	public void index(String indexPath,String docsPath) {
		try {
			// 1.创建Directory
			Directory dir = FSDirectory.open(new File(indexPath));//保存在硬盘上
			// 2.创建IndexWriter
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
			IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47,
					analyzer);
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);// 设置创建或追加模式
			IndexWriter writer = new IndexWriter(dir, iwc);
			final File docDir = new File(docsPath);
			indexDocs(writer, docDir);
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void indexing(IndexWriter writer) throws IOException {
		for (int i = 1; i <= 26; i++) {
			Document doc = new Document();
			doc.add(new StringField("id", "testid_" + i, Field.Store.YES));
			doc.add(new StringField("name", "name_" + i, Field.Store.YES));
			doc.add(new StringField("_version", "1", Field.Store.YES));
			Field uid = new StringField("_uid", "test#" + i, Field.Store.YES);
			//uid.setStringValue("uf#" + UUID.randomUUID().toString());
			doc.add(uid);
			writer.addDocument(doc);
		}
	}
    
	public void indexDocs(IndexWriter writer, File file) throws IOException {
		if (file.canRead()) {
			if (file.isDirectory()) {//如果是文件夹，则遍历文件夹内的所有文件
				String[] files = file.list();
				// an IO error could occur
				if (files != null) {
					for (int i = 0; i < files.length; i++) {
						indexDocs(writer, new File(file, files[i]));
					}
				}
			} else {//如果是文件
				FileInputStream fis;
				try {
					fis = new FileInputStream(file);
				} catch (FileNotFoundException fnfe) {
					return;
				}
				try {
					// 3.创建Document对象
					Document doc = new Document();
					// 4.为Document添加Field
					// Add the path of the file as a field named "path". Use a
					// field that is indexed (i.e. searchable), but don't
					// tokenize
					// the field into separate words and don't index term
					// frequency
					// or positional information:
					//以文件的文件路径建立Field
					Field pathField = new StringField("path", file.getPath(),Field.Store.YES);
					doc.add(pathField);//添加到文档中
					//以文件的名称建立索引域
					doc.add( new StringField("filename", file.getName(),Field.Store.YES));//添加到文档中
					// Add the last modified date of the file a field named
					// "modified".
					// Use a LongField that is indexed (i.e. efficiently
					// filterable with
					// NumericRangeFilter). This indexes to milli-second
					// resolution, which
					// is often too fine. You could instead create a number
					// based on
					// year/month/day/hour/minutes/seconds, down the resolution
					// you require.
					// For example the long value 2011021714 would mean
					// February 17, 2011, 2-3 PM.
					doc.add(new LongField("modified", file.lastModified(),Field.Store.YES));
					// Add the contents of the file to a field named "contents".
					// Specify a Reader,
					// so that the text of the file is tokenized and indexed,
					// but not stored.
					// Note that FileReader expects the file to be in UTF-8
					// encoding.
					// If that's not the case searching for special characters
					// will fail.
					//以文件的内容建立索引域（Field）
					doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(fis, "UTF-8"))));
					if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
						// New index, so we just add the document (no old
						// document can be there):
						System.out.println("adding " + file);
						writer.addDocument(doc);//将文档写入到索引中（以创建的方式）
					} else {
						// Existing index (an old copy of this document may have
						// been indexed) so
						// we use updateDocument instead to replace the old one
						// matching the exact
						// path, if present:
						System.out.println("updating " + file);		
						writer.updateDocument(new Term("path", file.getPath()),doc);//以追加方式写入到索引中
					}
				} finally {
					fis.close();
				}
			}
		}
	}
	/**
	 * 搜索
	 * http://blog.csdn.net/nuptboyzhb
	 */
	public void searcher(String indexPath){
		try {
			IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
			IndexSearcher searcher = new IndexSearcher(reader);
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
			String field = "contents";//搜索域是：文档的内容
			QueryParser parser = new QueryParser(Version.LUCENE_47, field, analyzer);
		    Query query= parser.parse("南京");//搜索内容中含有“南京”的文档
		    TopDocs tds=searcher.search(query, 10);//搜索前十个
		    ScoreDoc[] sds= tds.scoreDocs;
		    for (ScoreDoc sd:sds) {//将内容中含有“南京”关键字的文档遍历一遍
				Document document=searcher.doc(sd.doc);
				System.out.println("score:"+sd.score+"--filename:"+document.get("filename")+
						"--path:"+document.get("path")+"--time"+document.get("modified"));//打印检索结果中文档的路径
			}
		    reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void searcherSimple(String indexPath){
		try {
			IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
			IndexSearcher searcher = new IndexSearcher(reader);
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
		    
		    Query query = new QueryParser(Version.LUCENE_47, "id", analyzer).parse("testid_*");
		    
		    TopDocs tds=searcher.search(query, 10);
		    ScoreDoc[] sds= tds.scoreDocs;
		    for (ScoreDoc sd:sds) {
				Document document=searcher.doc(sd.doc);
				List<IndexableField> fields = document.getFields();
				System.out.print("score:" + sd.score);
				for(IndexableField field : fields){
					System.out.print(", " + field.name() + ":" + field.stringValue());
				}
				System.out.println("");
			}
		    reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
