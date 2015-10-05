package lucene.indexer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Date;
import au.com.bytecode.opencsv.CSVReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

//Class which reads the data from csv and preprocesses it
public class SearchTransformation {
	String files_to_index;
	String input_csv;
	String index_dir;
	HashSet<String> Id = new HashSet<String>();

	//Constructor
	public SearchTransformation( String FILES_TO_INDEX_DIRECTORY, String INPUT_CSV_FILE, String INDEX_DIRECTORY)
	{
		this.files_to_index = FILES_TO_INDEX_DIRECTORY;
		this.input_csv = INPUT_CSV_FILE;
		this.index_dir = INDEX_DIRECTORY;
	}

	private static final String Message = null;
	BufferedWriter OutFileWriter;
	//StringBuilder sentence = new StringBuilder();
	//custom made stop words. We can also use Lucene default stop words as well.I used this for performance reasons
	public static final List<String> stopwords =Arrays.asList("a", "I","am","about","no","br", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", 
			"alone", "along", "already", "also","although","always","am","among", "amongst", "amoungst", "amount",  "an", "and","i","to", 
			"another", "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be","became", "dont",
			"because","become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", 
			"between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt",
			"cry", "de", "describe", "detail", "do", "dont", "know","done", "down", "due", "during", "each", "eg",  "either", "else",
			"elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", 
			"fify", "fill", "find", "fire", "first", "for", "former", "formerly", "found", "from", 
			"front", "full", "further", "get", "give", "go", "had", "has", "hasnt","look","looking","wanted",
			"have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself","let",
			"him", "himself", "his", "how", "however", "ie", "if", "in", "inc", "indeed", "interest", "into", "'","m",
			"is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", 
			"may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "need",
			"my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", 
			"noone", "nor", "not", "nothing", "now", "nowhere", "or","off", "often", "on", "once", "one", "only", "onto", 
			"or", "other", "others", "otherwise", "our", "ours", "ourselves", "over", "own","part", "per", "perhaps",
			"please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she",
			"should", "show", "side", "since", "sincere", "so", "some", "somehow", "someone", "something", 
			"sometime", "sometimes", "somewhere", "still", "such", "system", "take", "than", "that", "the", "their", 
			"them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", 
			"these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", 
			"thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", 
			"up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever","search",
			"where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while","i am","up",
			"whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet",
			"you", "your", "yours", "yourself", "yourselves","1","2","3","4","5","6","7","8","9","10","1.","2.","3.","4.","5.","6.","11",
			"7.","8.","9.","12","13","14","terms","CONDITIONS","conditions","values","interested.","care","sure","a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z","i'm","search","want","dont","got","just",
			"contact","grounds","buyers","tried","said,","plan","value","principle.","forces","sent:","is,","was","like",
			"discussion","tmus","diffrent.","layout","area.","thanks","thankyou","hello","bye","rise","fell","fall","psqft.","km","miles");
	private static final String NULL = null;

	//preprocess function
	public void run() throws IOException {

		boolean result = false;
		try {
			File fl = new File(files_to_index);
			if (!fl.exists()) {
				result = fl.mkdir();
				if(result)
				{
					System.out.println("Sucessfully created the directory");
				}
				if(!result)
				{
					System.out.println("Error creating the files to index directory");
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		File f1 = new File(index_dir);
		if(f1.exists() && f1.isDirectory())
		{
		IndexReader read = DirectoryReader.open(FSDirectory.open(new File(index_dir)));
		int num = read.maxDoc();
		for ( int i = 0; i < num; i++)
		{
			Document d = read.document(i);
			Id.add(d.get("Key").toString());
		}
		read.close();
		}
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";

		CSVReader reader = null;
		try {
			// Get the CSVReader instance with specifying the delimiter to be
			// used
			reader = new CSVReader(new FileReader(input_csv), ',');
			String[] nextLine;
			String content = "null";
			StringBuffer con = new StringBuffer();
			int count = 0;
			String key = null;
			String message_file = null;
			// Read one line at a time
			while ((nextLine = reader.readNext()) != null) {
				if(count == 0)
				{count = count+1;}
				//logic to create a key out of email id and server timestamp and check for duplication
				else
				{
					String token = nextLine[1];
					String name = null;
					String time = null;
					String pattern3 = "(.*)Email(.*)";
					Pattern r3 = Pattern.compile(pattern3);
					Matcher m3 = r3.matcher(token);
					{
						if(m3.find())
						{
							name =m3.group(0).substring(0,46).replace("Email:", "").trim();
						}
					}
					String pattern4 = "(.*)Server Timestamp(.*)";
					Pattern r4 = Pattern.compile(pattern4);
					Matcher m4 = r4.matcher(token);
					if(m4.find())
					{
						//System.out.println(m1.group(0).replaceAll("Server Timestamp:","").substring(1,11));
						//b.append(m1.group(0).replaceAll("Server Timestamp:","").substring(1,11).replaceAll("-", ""));
						time = m4.group(0).replaceAll("Server Timestamp:","").substring(1,11).replaceAll("-", "");
						//System.out.println(m1.group(0).substring(0,28).replaceAll("-", ""));
						//System.out.println("Server time is fine");
					}
					key = name+time;
					//System.out.println(token);
					if(token != null)
					{
						if(Id.isEmpty() || !Id.contains(key))
						{	
							//&& !Id.contains(key)
						//System.out.println(token);
						message_file =files_to_index+"\\file"+name+".txt"; 
						String pattern1 = "(.*)Message(.*)";
						Pattern r = Pattern.compile(pattern1);
						// Write the message content to a file 
						Matcher m = r.matcher(token);
						if (m.find()) {
							//content = m.group(0).replaceAll("Message:", "");
							content = m.group(0);
							//System.out.println(content);
						}
						StringBuffer b = new StringBuffer(content.length());
						for(String s :content.toLowerCase().split("\\b")){
							if(!stopwords.contains(s)) 
								b.append(s );
						}
						b.append("\n");
						// Write the date of feeback to the file 
						String pattern = "(.*)Server Timestamp(.*)";
						Pattern r1 = Pattern.compile(pattern);
						Matcher m1 = r1.matcher(token);
						if(m1.find())
						{
							//System.out.println(m1.group(0).replaceAll("Server Timestamp:","").substring(1,11));
							//b.append(m1.group(0).replaceAll("Server Timestamp:","").substring(1,11).replaceAll("-", ""));
							b.append(m1.group(0).substring(0,28).replaceAll("-", ""));
							//System.out.println(m1.group(0).substring(0,28).replaceAll("-", ""));
							//System.out.println("Server time is fine");
						}
						b.append("\n");
			
						b.append("Key:"+key);
						//System.out.println(b.toString());
						if (b != null)
						{
							//System.out.println(b.toString());
							InitializeWriters(message_file);
							WriteToFile(b.toString());
							CleanupAfterFinish();
							count = count +1;
						}
					}
						
					}
				}
			}	
		}
		catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Initialize the file writer
	 * 
	 * @param path
	 *            of the file
	 * @param outFilename
	 *            name of the file
	 */
	public void InitializeWriters(String outFilename) {
		try {
			File fl = new File(outFilename);
			if (!fl.exists()) {
				fl.createNewFile();
			}
			if(fl.exists()){
				fl.delete();
			}
			/**
			 * Use UTF-8 encoding when saving files to avoid losing Unicode
			 * characters in the data
			 */
			OutFileWriter = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(outFilename, true), "UTF-8"));
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * Close the opened filewriter to save the data
	 * 
	 * @throws IOException
	 */
	public void CleanupAfterFinish() throws IOException {
		OutFileWriter.close();
	}

	/**
	 * Writes the retrieved data to the output file
	 * 
	 * @param data
	 *            containing the retrieved information in JSON
	 * @param key
	 *            name of the key currently being written
	 */
	public void WriteToFile(String data) {
		try {
			OutFileWriter.write(data);
			OutFileWriter.newLine();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
}
