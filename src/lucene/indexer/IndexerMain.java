package lucene.indexer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;




@SuppressWarnings("deprecation")
public class IndexerMain {
	String files_to_index;
	String indexPath;
	
	//Constructor which takes the input file path and directory of index creation
	public IndexerMain(String files_to_index, String indexPath)
	{
		this.files_to_index = files_to_index;
		this.indexPath = indexPath;
	}
	
	public static final String FIELD_PATH = "path";
	public static final String FIELD_CONTENTS = "contents";
	private static final String NULL = null;

	public static void main(String[] args) throws Exception {
		
		String usage = "java -cp Index_creation.jar lucene.indexer.IndexerMain"
                + " [-h help] [-index INDEX_PATH] [-input_file INPUT_CSV_FILE]\n"
                 + "This creates/updates Lucene index for the input csv file, if the index directory is not specified,\n"
                 + "Lucene index is created in the current directory as LuceneIndex"
                 ;
    String indexPath = null;
    String input_file = null;
    String files_to_index; 
    String phrases = "3"; // Hard coding the value to 3, so that we can create an index of Shingles of 3 worded.
    //String docsPath = null;
    //boolean create = true;
    for(int i=0;i<args.length;i++) {
    	if ("-h".equals(args[i])) {
    		 System.out.println("Usage: " + usage);
    		 System.exit(1);
    	}
    	else if ("-index".equals(args[i])) {
        indexPath = args[i+1];
        i++;
      } else if ("-input_file".equals(args[i])) {
        input_file = args[i+1];
        i++;
      } 
    }

    if (input_file == null ) {
    	System.err.println("Enter the input CSV file and rerun the tool again\n");
    	System.err.println("Usage: " + usage);
      System.exit(1);
    }
   
		// TODO Auto-generated method stub
	//	String files_to_index = System.getenv("files_to_index");
	//	String indexPath = System.getenv("indexPath");
	//	String Output_CSV_FILE = System.getenv("Output_CSV_FILE");
	//	String phrases = System.getenv("Number_of_phrases");
	//	String Input_CSV_FILE = System.getenv("INPUT_CSV_FILE");
		
		
		//Creating a default directory so that we can extract only the messages of the feedback mail into a txt file and parse that text to create an index
		/*if(files_to_index == NULL)
    	
		{
			files_to_index = "Extraction";
		}
		*/
		//Creating a default directory as LuceneIndex in the current path if env is not specified.
		files_to_index = "Extraction";
    	if(indexPath == NULL)
			indexPath= "LuceneIndex";
		
		//Preprocess the csv file 
		SearchTransformation st = new SearchTransformation(files_to_index,input_file,indexPath);
		st.run();
		IndexerMain ind = new IndexerMain(files_to_index,indexPath);
		ind.createIndex(phrases);
		File f = new File(files_to_index);
		ind.removeDirectory(f);
		//createIndex();
		
	}

	public void createIndex(String phrases) throws IOException {
		// FileInputStream fis;
		String temp = "";
		//@SuppressWarnings("deprecation")
		int val = Integer.parseInt(phrases);
		//Analyzer analyzer = new ShingleAnalyzerWrapper(new SnowballAnalyzer(Version.LUCENE_4_9, "English", CharArraySet.EMPTY_SET),val,val," ",false,false,null);
		//Using Lucene Analyzers based on the need	
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9, CharArraySet.EMPTY_SET);
		Analyzer analyzer1 = new ShingleAnalyzerWrapper(analyzer,val,val," ",false,false,null);
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_9,analyzer1); //(Version.LUCENE_40,analyzer);
		config.setOpenMode(OpenMode.CREATE_OR_APPEND);
		Directory dir;
		dir = FSDirectory.open(new File(indexPath));
		IndexWriter indexWriter;
		indexWriter = new IndexWriter(dir, config);
		final File docDir = new File(files_to_index);
		File[] files = docDir.listFiles();
		for (File file : files) {
			Document doc = new Document();
			Field pathField = new StringField("path", file.getPath(),Field.Store.YES);
			//Field.Store.YES);
			doc.add(pathField);
			//Add the date field into the index
			BufferedReader input = new BufferedReader(new FileReader(file));
			String content = null, line;
			String content1 = null;
			String content2 = null;
			StringBuffer b;
			String pattern1 = "(.*)message(.*)";
			String pattern2 = "(.*)Server Timestamp(.*)";
			Pattern r = Pattern.compile(pattern1);
			Pattern r1 = Pattern.compile(pattern2);
			Pattern r2 = Pattern.compile("(.*)Key(.*)");
			while ((line = input.readLine()) != null) {
				Matcher m = r.matcher(line);
				Matcher m1 = r1.matcher(line);
				Matcher m2 = r2.matcher(line);
				if (m.find()) {
					content = m.group(0).replaceAll("message:", "");
					//content = m.group(0);
					//System.out.println(content);
				}
				
				if(m1.find()) {
					content1 = (m1.group(0).replaceAll("Server Timestamp:",""));//.substring(1,3);
					
				}
				if(m2.find()) {
					content2 = (m2.group(0).replaceAll("Key:",""));//.substring(1,3);
				}
			}
			//System.out.println(content1);
			//System.out.println(content);
			doc.add(new Field("contents", content,Field.Store.YES, Field.Index.ANALYZED));
			Long date = Long.valueOf(content1.trim());
				//int date =Integer.parseInt(content1.trim());
			doc.add(new LongField("dateoffeedback", date,Field.Store.YES));
			doc.add(new Field("Key",content2,Field.Store.YES,Field.Index.ANALYZED));
			//doc.add(new StringField("dateoffeedback",content1,Field.Store.YES));		
			//doc.add(new Field("date of feedback",last,Field.Store.YES,Field.Index.NO));
			input.close();
			indexWriter.addDocument(doc);
		}
		
		indexWriter.commit();
		indexWriter.close();
		
	}
	//remove the directory
	public boolean removeDirectory(File directory) {

		  // System.out.println("removeDirectory " + directory);

		  if (directory == null)
		    return false;
		  if (!directory.exists())
		    return true;
		  if (!directory.isDirectory())
		    return false;

		  String[] list = directory.list();

		  // Some JVMs return null for File.list() when the
		  // directory is empty.
		  if (list != null) {
		    for (int i = 0; i < list.length; i++) {
		      File entry = new File(directory, list[i]);

		      //        System.out.println("\tremoving entry " + entry);

		      if (entry.isDirectory())
		      {
		        if (!removeDirectory(entry))
		          return false;
		      }
		      else
		      {
		        if (!entry.delete())
		          return false;
		      }
		    }
		  }

		  return directory.delete();
		}
	
	//the below is not required
	/*public void search(String searchString) throws Exception {

		//System.out.println("Searching for '" + searchString + "'");
		IndexReader indexReader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
		Analyzer analyzer = new SnowballAnalyzer(Version.LUCENE_4_9, "English");

		IndexSearcher indexSearcher = new IndexSearcher(indexReader);
		QueryParser queryParser = new QueryParser(Version.LUCENE_4_9, "contents", analyzer);
		Query query = queryParser.parse(searchString);

		TermStats[] commonTerms = HighFreqTerms.getHighFreqTerms(indexReader,
				1000, "contents", new HighFreqTerms.TotalTermFreqComparator()); 

		CSVWriter writer = new CSVWriter(new FileWriter(Output_CSV_FILE));
		writer.writeNext(new String[] {"Popular phrase", "Count", "Date of feedback mail"});
		for (TermStats commonTerm : commonTerms) {
			//System.out.println("I am here");
			//System.out.println(commonTerm.termtext.utf8ToString());
			//System.out.println(commonTerm.totalTermFreq);
			writer.writeNext(new String[] {commonTerm.termtext.utf8ToString(),Long.toString(commonTerm.totalTermFreq)});

		} 
		
		writer.close();
		indexReader.close();
	}*/
}





