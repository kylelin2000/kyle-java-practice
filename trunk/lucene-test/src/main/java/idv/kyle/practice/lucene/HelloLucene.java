package idv.kyle.practice.lucene;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.index.lucene.FileSystemDirectory; 
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path; 

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


public class HelloLucene {
	public void indexLocalFile(String indexPath) {
		try {
			Directory dir = FSDirectory.open(new File(indexPath));
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
			IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			IndexWriter writer = new IndexWriter(dir, iwc);
			indexDoc(writer);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void indexHDFS(String indexPath) {
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			FileSystemDirectory dir = new FileSystemDirectory(fs, new Path(" input/index/ "), false, conf);
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
			IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			IndexWriter writer = new IndexWriter(dir, iwc);
			indexDoc(writer);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void indexDoc(IndexWriter writer) throws IOException {
		for (int i = 1; i <= 10000000; i++) {
			Document doc = new Document();
			doc.add(new StringField("id", "testid_" + i, Field.Store.YES));
			doc.add(new StringField("name", "name_" + i, Field.Store.YES));
			doc.add(new StringField("user", "user_" + i, Field.Store.YES));
			doc.add(new StringField("location", "location_" + i, Field.Store.YES));
			doc.add(new StringField("_version", "1", Field.Store.YES));
			Field uid = new StringField("_uid", "test#" + i, Field.Store.YES);
			//uid.setStringValue("uf#" + UUID.randomUUID().toString());
			doc.add(uid);
			writer.addDocument(doc);
		}
	}
	
	public void searchAll(String indexPath){
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
