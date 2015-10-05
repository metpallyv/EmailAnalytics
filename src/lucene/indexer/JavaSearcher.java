package lucene.indexer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.analysis.snowball.SnowballAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.misc.HighFreqTerms;
import org.apache.lucene.misc.TermStats;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldCache.Bytes;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.index.memory.*;

import au.com.bytecode.opencsv.CSVWriter;

public class JavaSearcher {
	private static final String NULL = null;
	String[] out_format;// = new String[9999];
	String[] range_values; // = new String[9999];
	String indexPath = null;
	int num;
	int minterm;
	String grouping;
	String num_phrases;
	String output_file;
	String minTermCount ;
	String start_range;
	String stop_range ;
	int month_grouping ;
	int month_start;
	int month_stop;
	int var;
	int var1;
	int flag;
	public JavaSearcher(String indexPath2, int num2, int minterm2,
			String grouping2, String num_phrases2, String output_file2,
			String minTermCount2, String start_range2, String stop_range2, int month_grouping) {
		this.indexPath = indexPath2;
		this.num = num2;
		this.minterm = minterm2;
		this.month_grouping = month_grouping;
		this.num_phrases = num_phrases2;
		this.output_file = output_file2;
		this.minTermCount = minTermCount2;
		this.start_range = start_range2;
		this.stop_range = stop_range2;
		this.out_format = new String[9999];
		this.range_values = new String[9999];

	}
	/**
	 * @param args
	 * @throws Exception 
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {


		String usage = "java -cp Feedback_term_report.jar lucene.indexer.JavaSearcher"
				+ " [-h help] [-index INDEX_PATH] [-num_phrases number] [-minTermCount number] [-grouping month/year] [-startDate yymmdd] [-endDate yymmdd] [-output_file OUTPUT_CSV_FILE]\n\n"
				+ "Explanation of the tool:This tool searches for most occurring phrases in Lucene index for the given date range" +
				" and returns the occurrence of those phrases month/year \n\n"
				+"Options:\n"
				+"-h help - This is the help command on how to run this tool\n"
				+"-index INDEX_PATH - Specify the Lucene Index location on which you want to run this tool. Default Index is LuceneIndex in the current directory\n"
				+"-num_phrases number - This extracts the Top n terms from the lucene index for the specified date range. Default is 50\n"
				+"-minTermCount number -Number of minimum terms that must appear in a given grouping for it to appear in the result set.Default is 5\n"
				+"-grouping month/year -Determines how the aggregate results are reported. Default is month\n"
				+"-startDate yymmdd- Mandatory paramter which is the start date for the feedback_term_report. Note that the format should be like yymmdd(Ex:20140401)\n"
				+"-endDate yymmdd- Mandatory paramter which is the end date for the feedback_term_report. Note that the format should be like yymmdd(Ex:20140830)\n"
				+"-output_file OUTPUT_CSV_FILE -Filename to output the result set to. Default is Feedback_term_report.CSV in the current directory";
		String indexPath = null;
		int num = 0;
		int minterm = 0;
		String grouping = null;
		String num_phrases = null;
		String output_file = null;
		String minTermCount = null;
		String start_range = null;
		String stop_range = null; 
		int month_grouping = 0;
		for(int i=0;i<args.length;i++) {
			if ("-h".equals(args[i])) {

				System.out.println("Usage: " + usage);
				System.exit(1);
			}
			else if ("-num_phrases".equals(args[i])) {

				num_phrases = args[i+i];
				System.out.println(num_phrases);
				i++;
			}else if ("-index".equals(args[i])) {
				indexPath = args[i+1];
				i++;
			}
			else if ("-minTermCount".equals(args[i])) {
				minTermCount = args[i+1];

				i++;
			}
			else if ("-grouping".equals(args[i])) {
				grouping = args[i+1];
				i++;
			}
			else if ("-startDate".equals(args[i])) {
				start_range = args[i+1];
				i++;
			}
			else if ("-endDate".equals(args[i])) {
				stop_range = args[i+1];
				i++;
			}
			else if ("-output_file".equals(args[i])) {
				output_file = args[i+1];
				i++; }
		}
		if ((start_range == null ) || (stop_range == null)) {
			System.err.println("Enter the Date range values and rerun the tool again\n");
			System.err.println("Usage: " + usage);
			System.exit(1);
		}
		// JavaSearcher js = new JavaSearcher(indexPath,minTermCount,num_phrases,grouping,output_file);

		if(indexPath == NULL)
			indexPath= "LuceneIndex";

		//int month_grouping ;

		if (minTermCount != null)
			minterm = Integer.parseInt(minTermCount);

		if(num_phrases != null)
			num = Integer.parseInt(num_phrases);

		if (minTermCount == null)
			minterm = 5;

		if(num_phrases == null)
		{
			num = 50;//Returning the top 50 phrases from the index by default
		}


		if(grouping != null && grouping.trim().equals("year"))
		{
			month_grouping = 1;
		}
		else {
			month_grouping = 0;
		}
		if(output_file == null)
		{
			output_file = "feedback_term_report.CSV";
		}

		JavaSearcher js = new JavaSearcher(indexPath,num,minterm,grouping,num_phrases,output_file,minTermCount,start_range,stop_range,month_grouping);
		//	js.search(num_phrases,indexPath,minterm,num,month_grouping,output_file,start_range,stop_range);
		js.search();
	}
	//This logic creates a reader to retrieves all the docs between the date range so that then we can
	//create a temporary lucene index to retrieve the top phrases within that range
	//An alternate of doing this without using the temp index would be possible if we could termVector
	//values within the index. In lucene 4.9, I was unable to extract the term vectors for each doc
	//public void search(String num_phrases, String indexPath, int minterm, int num, int month_grouping, String output_file, String start_range, String stop_range) throws Exception
	public void search() throws Exception
	{
		long start = Long.valueOf(start_range.trim());
		long stop = Long.valueOf(stop_range.trim());
		IndexReader indexReader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
		IndexSearcher indexSearcher = new IndexSearcher(indexReader);
		Query rangeQuery = NumericRangeQuery.newLongRange("dateoffeedback",start,stop,true,true);

		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9, CharArraySet.EMPTY_SET);
		Analyzer analyzer1 = new ShingleAnalyzerWrapper(analyzer,3,3," ",false,false,null);
		//Analyzer analyzer = new ShingleAnalyzerWrapper(new SnowballAnalyzer(Version.LUCENE_4_9, "English", CharArraySet.EMPTY_SET),3,3," ",false,false,null);
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_9,analyzer1); //(Version.LUCENE_40,analyzer);
		config.setOpenMode(OpenMode.CREATE);
		IndexWriter indexWriter;
		Directory idx = new RAMDirectory();
		indexWriter = new IndexWriter(idx, config);
		TopDocs topDocs = indexSearcher.search(rangeQuery, 999999999);

		// I am adding the contents of the documents within the range into a temp index below
		for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
			Document new_doc = new Document();
			Document doc = indexSearcher.doc(scoreDoc.doc);
			new_doc.add(new TextField("contents", doc.get("contents").toString(),Field.Store.YES));
			Long date = Long.valueOf(doc.get("dateoffeedback").toString());
			new_doc.add(new LongField("dateoffeedback", date,Field.Store.YES));
			indexWriter.addDocument(new_doc);

		}

		indexReader.close();
		indexWriter.commit();
		indexWriter.close();
		IndexReader idxReader = DirectoryReader.open(idx);

		//retreive_phrases(idx, idxReader, num,output_file, minterm,month_grouping,start_range,stop_range);
		retreive_phrases(idx,idxReader);
		idxReader.close();
		idx.close();

	}

	//Method which retrieves the data and its aggregated values
	//public void retreive_phrases(Directory idx,IndexReader idxReader, int num, String output_file, int minterm, int month_grouping, String start_range, String stop_range) throws Exception
	public void retreive_phrases(Directory idx, IndexReader idxReader) throws Exception
	{
		TermStats[] commonTerms = HighFreqTerms.getHighFreqTerms(idxReader,
				num, "contents", new HighFreqTerms.TotalTermFreqComparator()); 
		// String[] out_format = new String[9999];

		CSVWriter writer = new CSVWriter(new FileWriter(output_file));
		out_format[0] = "Popular phrase";
		out_format[1] = "Total Count";
		flag = 0;
		//writer.writeNext(new String[] { );
		for (TermStats commonTerm : commonTerms) {
			if(minterm <= commonTerm.totalTermFreq)
			{
				//System.out.println("Month grouping value"+month_grouping);
				var = 0;
				var1 = 2;
				range_values[0] = commonTerm.termtext.utf8ToString();
				range_values[1] = Long.toString(commonTerm.totalTermFreq);
				var = var+2;
				IndexReader idxReader1 = DirectoryReader.open(idx);
				IndexSearcher searcher = new IndexSearcher(idxReader1);
				Analyzer analyzer2 = new ShingleAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_4_9, CharArraySet.EMPTY_SET),3,3," ",false,false,null);
				QueryParser queryParser = new QueryParser(Version.LUCENE_4_9,"contents",analyzer2);
				queryParser.setDefaultOperator(QueryParser.Operator.AND);
				queryParser.setEnablePositionIncrements(true);
				String str = commonTerm.termtext.utf8ToString();
				String[] s = str.split(" ");
				StringBuilder querystr = new StringBuilder(128);
				for(String st: s)
				{
					querystr.append(st + "+");
				}
				Query query = queryParser.parse(querystr.toString());
				//Data analysis for occurrence of each top keyword in year field

				int year_ulimit = Integer.valueOf(start_range.trim().substring(0,4));
				int year_llimit = Integer.valueOf(stop_range.trim().substring(0,4));

				month_start = Integer.valueOf(start_range.trim().substring(4,6));
				month_stop = Integer.valueOf(stop_range.trim().substring(4,6));


				if(month_grouping == 0)
				{	//This part executes when month grouping is true
					if (year_ulimit >= year_llimit )
					{
						if (year_ulimit > year_llimit)
						{
							while ((month_start <= 12) && (month_stop > month_start))
							{
								BooleanQuery booleanQuery = new BooleanQuery();
								if(month_start <10)
								{	int val = 0;
								String temp_start = Integer.toString(year_llimit)+"0"+Integer.toString(month_start)+"01";
								String temp_stop =  Integer.toString(year_llimit)+"0"+Integer.toString(month_start)+"31";
								search_range(temp_start, temp_stop, booleanQuery,query,val,searcher,year_llimit,str);
								}

								else
								{	int val = 0;
								String temp_start = Integer.toString(year_llimit)+Integer.toString(month_start)+"01";
								String temp_stop =  Integer.toString(year_llimit)+Integer.toString(month_start)+"31";
								search_range(temp_start, temp_stop, booleanQuery,query,val,searcher,year_llimit,str);
								}
								year_llimit = year_llimit+1;
								month_start = 1;
							}
						}
						else {
							if(year_ulimit == year_llimit)
							{
								while(month_start <= month_stop)
								{
									BooleanQuery booleanQuery = new BooleanQuery();
									if(month_start < 10)
									{	int val = 0;
									String temp_start = Integer.toString(year_llimit)+"0"+Integer.toString(month_start)+"01";
									String temp_stop =  Integer.toString(year_llimit)+"0"+Integer.toString(month_start)+"31";
									search_range(temp_start, temp_stop, booleanQuery,query,val,searcher,year_llimit,str);
									}
									else
									{	int val = 0;
									String temp_start = Integer.toString(year_llimit)+Integer.toString(month_start)+"01";
									String temp_stop =  Integer.toString(year_llimit)+Integer.toString(month_start)+"31";
									search_range(temp_start, temp_stop, booleanQuery,query,val,searcher,year_llimit,str);

									month_start = month_start+1;
									}
								}
							}
						}

					}
					if(flag == 0)
					{
						writer.writeNext(out_format);
						flag = 1;
					}
					writer.writeNext(range_values);
				}

				else 
				{
					//This part executes when we want to aggregate data by year
					if (year_ulimit >= year_llimit )
					{
						if(year_ulimit > year_llimit)
						{ int val = 0;
						BooleanQuery booleanQuery = new BooleanQuery();
						String temp_start = Integer.toString(year_llimit);
						String temp_stop =  Integer.toString(year_llimit);
						search_range(temp_start, temp_stop, booleanQuery,query,val,searcher,year_llimit,str);
						}
						if(year_ulimit == year_llimit)
						{
							int val = 0;
							BooleanQuery booleanQuery = new BooleanQuery();
							String temp_start = Integer.toString(year_llimit)+"0101";
							String temp_stop =  Integer.toString(year_llimit)+"1231";
							//search_range(temp_start,temp_stop,booleanQuery,query,val,flag,searcher,var,var1,month_start,year_llimit,str);
							search_range(temp_start, temp_stop, booleanQuery,query,val,searcher,year_llimit,str);
						}

						if(flag == 0)
						{
							writer.writeNext(out_format);
							flag = 1;
						}
						writer.writeNext(range_values);
					}
				}
			}
		}
		writer.close();	
	}
	//Actaul logic which searches for the phrase 
	public void search_range(String temp_start, String temp_stop, BooleanQuery booleanQuery, Query query, int val,IndexSearcher searcher,int year_llimit, String str) throws IOException

	{
		final String [] month = {null,  
				"January", "February", "March", "April", "May", "June", "July",  
				"August", "September", "October", "November", "December"};
		long start1 = Long.valueOf(temp_start.trim());
		long stop1 = Long.valueOf(temp_stop.trim());
		Query query2 = NumericRangeQuery.newLongRange("dateoffeedback",start1,stop1,true,true);
		booleanQuery.add(query,BooleanClause.Occur.MUST);
		booleanQuery.add(query2,BooleanClause.Occur.MUST);
		//System.out.println("Query is "+booleanQuery.toString());
		TopDocs topDocs1 = searcher.search(booleanQuery, 999999999);
		Pattern p = Pattern.compile(str);
		for (ScoreDoc scoreDoc : topDocs1.scoreDocs) {
			//Document doc = indexSearcher.doc(scoreDoc.doc);
			Document doc = searcher.doc(scoreDoc.doc);
			//Document doc = searcher.doc(scoreDoc);
			//Wish I could directly get the number of time my phrase exist in all the hit documents instead of this pattern matching
			String temp = doc.get("contents");
			Pattern SPECIAL_REGEX_CHARS = Pattern.compile("[{}()\\[\\].+*?^$\\\\|!-</>]");
			temp = SPECIAL_REGEX_CHARS.matcher(temp).replaceAll("");
			temp = temp.replaceAll("\\s+", " ").trim();
			Matcher m = p.matcher(temp);

			while(m.find()) {
				//System.out.println("Success");
				val = val+1;
			}

		}
		if(flag == 0)
		{
			if (month_grouping == 0)
				out_format[var1] = month[month_start]+year_llimit;
			else
				out_format[var1] = Integer.toString(year_llimit);
		}
		range_values[var] = Integer.toString(val);
		//count.add(month[month_start]+":"+topDocs1.scoreDocs.length);
		month_start = month_start+1;
		var = var+1;
		var1 = var1+1;
	}
}
