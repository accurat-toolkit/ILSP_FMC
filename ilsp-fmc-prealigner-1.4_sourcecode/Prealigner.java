package gr.ilsp.fmc.prealigner.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;



import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;



public class Prealigner {

	private PrealignerOptions options;
	private Logger LOGGER = Logger.getLogger(Prealigner.class);

	public Prealigner(String[] args){
		options = new PrealignerOptions(LOGGER);
		options.parseOptions(args);
		// TODO Auto-generated method stub
		RollingFileAppender app = new RollingFileAppender();
		app.setFile(options.get_logfile());
		//app.setFile("log.txt");
		app.setMaxFileSize("10MB");
		app.setMaxBackupIndex(10);
		PatternLayout lay = new PatternLayout();
		lay.setConversionPattern("%-5p %d{yy/MM/dd HH:mm:ss}  %c %x - %m%n");
		app.setLayout(lay);	
		app.setName("FileAppender");
		app.activateOptions();
		LOGGER.addAppender(app);				
	}

	public static void main(String[] args) {
		Prealigner align = new Prealigner(args);
		align.start();	// TODO Auto-generated method stub

	}

	private void start() {
		long start = System.currentTimeMillis();
		HashMap<String,Integer> t1 = new HashMap<String,Integer>();
		HashMap<String,Integer> t2 = new HashMap<String,Integer>();

		HashMap<String,HashMap<Integer,Integer>> sigs1 = new HashMap<String,HashMap<Integer,Integer>>();
		HashMap<String,HashMap<Integer,Integer>> sigs2 = new HashMap<String,HashMap<Integer,Integer>>();
		
		LOGGER.info("Reading first topic file...");
		t1 = parseTopic(options.get_topic1());
		LOGGER.info("Reading second topic file...");
		t2 = parseTopic(options.get_topic2());
		
		/*HashMap<String,ArrayList<String>> ex1 = new HashMap<String,ArrayList<String>>();
		LOGGER.info("Parsing first language exports...");
		ex1 = parseExports(options.get_input1(),t1);		
		LOGGER.info("Parsing second language exports...");
		parseSecond(ex1,options.get_input2(),t2,options.get_output());*/
		

		LOGGER.info("Parsing first language exports...");
		sigs1 = createSignatures(options.get_input1(),t1);
		LOGGER.info("Parsing second language exports...");
		sigs2 = createSignatures(options.get_input2(),t2);
		LOGGER.info("Comparing...");
		compareSignatures(sigs1,sigs2);
		LOGGER.info("Finished in " + (System.currentTimeMillis()-start) + " milliseconds.");
	}

	private void compareSignatures(
			HashMap<String, HashMap<Integer, Integer>> sigs1,
			HashMap<String, HashMap<Integer, Integer>> sigs2) {		
		HashMap<Integer,Integer> s1 = null;
		HashMap<Integer,Integer> s2 = null;
		BufferedWriter wrt = null;
		try {
			wrt = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(options.get_output())));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}		
		int threshold = options.get_threshold();
		int filecount = 0;
		int common = 0, tot = 0;
		for (String f1:sigs1.keySet()){
			filecount++;
			System.out.print("Checking file " + filecount + "\r");
			s1 = sigs1.get(f1);
			for (String f2:sigs2.keySet()){
				common = 0;
				tot = 0;
				s2 = sigs2.get(f2);
				for (Integer i1:s1.keySet()){
					tot += s1.get(i1);
					if (s2.containsKey(i1)){
						common += Math.min(s1.get(i1), s2.get(i1));
					}
				}
				if (common==0) continue;
				try {
					/*if (((double)common/tot)>0.8){
						wrt.write(f1 + "\t" + f2 + "\t5\r\n");
					} else if (((double)common/tot)>0.6 && threshold<5){
						wrt.write(f1 + "\t" + f2 + "\t4\r\n");
					} else if (((double)common/tot)>0.4 && threshold<4){
						wrt.write(f1 + "\t" + f2 + "\t3\r\n");
					}*/
					if (common>=threshold){
						wrt.write(f1 + "\t" + f2 + "\t"+common+"\r\n");
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}					
			}			
		}
		try {
			wrt.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void parseSecond(HashMap<String, ArrayList<String>> ex1,
			String input, HashMap<String, Integer> topic, String output) {
		File f = new File(input);
		if (!f.isDirectory()){
			LOGGER.error("Not valid export directory: " + input);
			return;
		}
		File o = new File(output);
		if (o.exists()){
			o.delete();			
		}
		BufferedWriter wrt = null;
		try {
			wrt = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(o),"UTF-8"));
		} catch (UnsupportedEncodingException e1) {
			LOGGER.error("UnsupportedEncodingException for: " + output);
			return;
		} catch (FileNotFoundException e1) {
			LOGGER.error("FileNotFoundException for: " + output);
			return;
		}
		BufferedReader rdr = null;
		String line = "";
		String terms = "";
		String signature = "";
		int score;
		int count = 0;
		int pairsCount = 0;
		int filesPaired = 0;
		for (File s:f.listFiles(new XmlFilter())){			
			count++;
			System.out.print("Parsing: " + count + "\r");			
			try {
				rdr = new BufferedReader(new InputStreamReader(new FileInputStream(s),"UTF-8"));
			} catch (UnsupportedEncodingException e) {
				LOGGER.error("UnsupportedEncodingException for: " + s);
				return;
			} catch (FileNotFoundException e) {
				LOGGER.error("FileNotFoundException for: " + s);
				return;
			}
			signature = "";
			score = 0;
			try {
				while ((line=rdr.readLine())!=null){
					if (line.matches(".*topic=\".*\".*")){
						terms = line.substring(line.indexOf("topic=")+7,line.indexOf("\">"));
						for (String t:terms.split(";")){
							if (t.trim().length()==0) continue;
							signature += topic.get(t.trim());
							score++;
						}
					}					
				}
				rdr.close();
				if (signature.length()==0) continue;
				if (ex1.containsKey(signature)){
					pairsCount += ex1.get(signature).size();
					filesPaired++;
					writePairs(s.getName(),ex1.get(signature),wrt,score);
				}
			} catch (IOException e) {
				LOGGER.error("IOException for: " + s);
				return;
			}
		}
		try {
			wrt.close();
		} catch (IOException e) {
			LOGGER.error("IOException for: " + output);
			return;
		}
		LOGGER.info(count + " files processed.");
		LOGGER.info(filesPaired + " files have a match.");
		LOGGER.info(pairsCount + " possible pairs found.");
	}

	private void writePairs(String name, ArrayList<String> files,
			BufferedWriter wrt, int score) {
		String line = "";
		for (String s:files){
			line = name + "\t" + s + "\t" + score + "\r\n";
			try {
				wrt.write(line);
			} catch (IOException e) {
				LOGGER.error("IOException for: " + s);
				return;
			}
		}

	}

	private HashMap<String, ArrayList<String>> parseExports(String file,
			HashMap<String, Integer> topic) {
		ArrayList<String> files = null;
		HashMap<String, ArrayList<String>> res = new HashMap<String, ArrayList<String>>();
		File f = new File(file);
		if (!f.isDirectory()){
			LOGGER.error("Not valid export directory: " + file);
			return null;
		}
		BufferedReader rdr = null;
		String line = "";
		String terms = "";
		String signature = "";
		int count = 0;
		for (File s:f.listFiles(new XmlFilter())){
			count++;
			System.out.print("Parsing: " + count + "\r");			
			try {
				rdr = new BufferedReader(new InputStreamReader(new FileInputStream(s),"UTF-8"));
			} catch (UnsupportedEncodingException e) {
				LOGGER.error("UnsupportedEncodingException for: " + s);
				return null;
			} catch (FileNotFoundException e) {
				LOGGER.error("FileNotFoundException for: " + s);
				return null;
			}
			signature = "";
			try {
				while ((line=rdr.readLine())!=null){
					if (line.matches(".*topic=\".*\".*")){
						terms = line.substring(line.indexOf("topic=")+7,line.indexOf("\">"));
						for (String t:terms.split(";")){
							if (t.trim().length()==0) continue;
							signature += topic.get(t.trim());
						}
					}					
				}
				rdr.close();
				if (signature.length()==0) continue;
				if (res.containsKey(signature)) {					
					files = res.get(signature);
					files.add(s.getName());
				} else {
					files = new ArrayList<String>();
					files.add(s.getName());
					res.put(signature, files);
				}
			} catch (IOException e) {
				LOGGER.error("IOException for: " + file);
				return null;
			}
		}
		return res;
	}

	private class XmlFilter implements FileFilter {

		public boolean accept(File f) {
			return (f.getName().endsWith(".xml"));			
		}

	}

	private HashMap<String,Integer> parseTopic(String file){
		HashMap<String,Integer> res = new HashMap<String,Integer>();
		File f = new File(file);
		if (!f.isFile()) {
			LOGGER.error("Not valid topic: " + file);
			return null;
		}
		String line = "";
		String term = "";
		int id = 0;
		BufferedReader rdr = null;
		try {
			rdr = new BufferedReader(new InputStreamReader(new FileInputStream(f),"UTF-8"));
		} catch (UnsupportedEncodingException e) {
			LOGGER.error("UnsupportedEncodingException for: " + file);
			return null;
		} catch (FileNotFoundException e) {
			LOGGER.error("FileNotFoundException for: " + file);
			return null;
		}
		try {
			while ((line=rdr.readLine())!=null){
				if (line.indexOf("\t")>0){
					term = line.substring(line.indexOf(": ")+2,line.indexOf("="));
					id = Integer.parseInt(line.substring(line.indexOf("\t")+1));
					res.put(term, id);
				}
			}
			rdr.close();
		} catch (IOException e) {
			LOGGER.error("IOException for: " + file);
			return null;
		}
		return res;
	}

	private HashMap<String,HashMap<Integer,Integer>> createSignatures(String file,HashMap<String, Integer> topic){
		HashMap<String,HashMap<Integer,Integer>> signatures = new HashMap<String,HashMap<Integer,Integer>>();
		HashMap<Integer,Integer> signature = null;
		File f = new File(file);
		if (!f.isDirectory()){
			LOGGER.error("Not valid export directory: " + file);
			return null;
		}
		BufferedReader rdr = null;
		String line = "";
		String terms = "";
		int count = 0;
		int id = 0;
		for (File s:f.listFiles(new XmlFilter())){
			count++;
			System.out.print("Parsing: " + count + "\r");			
			try {
				rdr = new BufferedReader(new InputStreamReader(new FileInputStream(s),"UTF-8"));
			} catch (UnsupportedEncodingException e) {
				LOGGER.error("UnsupportedEncodingException for: " + s);
				return null;
			} catch (FileNotFoundException e) {
				LOGGER.error("FileNotFoundException for: " + s);
				return null;
			}
			signature = new HashMap<Integer,Integer>();
			try {
				while ((line=rdr.readLine())!=null){
					if (line.matches(".*topic=\".*\".*")){
						terms = line.substring(line.indexOf("topic=")+7,line.indexOf("\">"));
						for (String t:terms.split(";")){
							if (t.trim().length()==0 || !topic.containsKey(t.trim())) continue;
							id = topic.get(t.trim());
							if (signature.get(id)!=null){
								signature.put(id, signature.get(id)+1);
							} else {
								signature.put(id,1);
							}
						}
					}
				}
				rdr.close();				
				signatures.put(s.getParentFile().getName() + File.separator + s.getName().replace(".xml",".txt"), signature);
			} catch (IOException e) {
				LOGGER.error("IOException for: " + file);
				return null;
			}
		}
		return signatures;
	}
}
