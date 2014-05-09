package idv.kyle.practice.lucene;
import org.junit.Test;

public class HelloLuceneTest {
	@Test
	public void TestIndexSimple() {
		HelloLucene hLucene = new HelloLucene();
		hLucene.indexLocalFile("/tmp/idx1");
	}

	@Test
	public void TestIndex() {
		HelloLucene hLucene = new HelloLucene();
		hLucene.index("/tmp/idx2", "/tmp/mydoc");
	}

	@Test
	public void TestSearcher() {
		HelloLucene hLucene = new HelloLucene();
		hLucene.searcher("/tmp/idx1");
	}

	@Test
	public void TestSearcherSimple() {
		HelloLucene hLucene = new HelloLucene();
		hLucene.searcherSimple("/tmp/idx1");
	}
}
