package gr.ilsp.fmc.classifier;

import gr.ilsp.fmc.datums.ClassifierDatum;
import gr.ilsp.fmc.datums.ExtendedParsedDatum;
import gr.ilsp.fmc.utils.TopicTools;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.tika.language.LanguageIdentifier;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;







@SuppressWarnings("serial")
public class Classifier implements Serializable{
	private static final Logger LOGGER = Logger.getLogger(Classifier.class);

	private double TITLE_WEIGHT = 10;
	private double KEYWORDS_WEIGHT = 4;
	private double META_WEIGHT = 2;
	private double CONTENT_WEIGHT = 1;
	private double TOTABSCORE_TH = 0.0;
	private double TOTRELSCORE_TH = 0.2;
	private double SUBCLASSSCORE_TH = 0.2;
	private String _targetLanguage;

	private String[] _classes;

	private ArrayList<String[]> _topic;

	private double _thres;
	private static int _min_uniq_terms;
	private boolean _keepBoiler = false;

	private int _max_depth;


	private static String langIdentified;

	public Classifier(String language, String[] classes, ArrayList<String[]> topic, double thres, boolean keepBoiler, int min_uniq_terms, int max_depth){
		_targetLanguage = language;
		_classes = classes;
		_topic = topic;
		_thres = thres;
		_keepBoiler  = keepBoiler;
		_min_uniq_terms = min_uniq_terms;
		_max_depth = max_depth;
	}
	public ClassifierDatum classify(ExtendedParsedDatum parsedDatum) {
		//String lang = parsedDatum.getLanguage();
		//System.out.println(lang);
		//System.out.println("Classifying " + parsedDatum.getUrl());
		String title = parsedDatum.getTitle();
		Map<String,String> metaMap = parsedDatum.getParsedMeta();
		String keywords = "";
		String meta = "";
		String content = parsedDatum.getParsedText();
		//if (_keepBoiler) content = cleanContent(content);
		content = cleanContent(content);
		
		String url = parsedDatum.getUrl();
		//		for (String s:metaMap.keySet()){
		//			if (s.equals("keywords"))
		//				keywords = metaMap.get(s);
		//			if (s.equals("description"))
		//				meta = metaMap.get(s);			
		//		}
		for (Entry<String, String> s:metaMap.entrySet()){
			if (s.getKey().equals("keywords"))
				keywords = s.getValue();
			if (s.getKey().equals("description"))
				meta = s.getValue();			
		}
		//guess language of the content
		if (_targetLanguage!=null){
			LanguageIdentifier LangI=new LanguageIdentifier(content); 
			langIdentified = LangI.getLanguage();
													
			String[] langs = _targetLanguage.split(";");
			boolean match = false;
			for (String lang:langs){
				if (langIdentified.equals(lang)) match = true;
			}
			//if (!match) return null;
			if (!match && content.length()>20){
				Detector detector = null;			
				try {
					detector = DetectorFactory.create();
					detector.append(content);
					langIdentified = detector.detect();										
				} catch (LangDetectException e) {
					langIdentified = "";
				}	
				for (String lang:langs){
					if (langIdentified.equals(lang)) match = true;
				}				
			}
			if (!match) return null;
		}
		if (_topic==null){
			return new ClassifierDatum(url, new String[0],new Double[0][0], 0.0, 0.0);
		}		
		if (title==null) title = "";
		
		Double[][] titleScores = rankText(title,TITLE_WEIGHT ,_topic,_classes,false);
		Double[][] keywordsScores=rankText(keywords,KEYWORDS_WEIGHT ,_topic,_classes,false);
		Double[][] metaScores=rankText(meta,META_WEIGHT ,_topic,_classes,false);		
		Double[][] contentScores=rankText(content,CONTENT_WEIGHT ,_topic,_classes, true);
		//System.out.println("Score:" + titleScores[titleScores.length-1][0] + " " +contentScores[contentScores.length-1][0]);
		ClassifierDatum result=classifyText(titleScores,keywordsScores,metaScores,contentScores,
				TOTABSCORE_TH,TOTRELSCORE_TH,SUBCLASSSCORE_TH,_classes, url);
		//System.out.println(parsedDatum.getUrl() + " " + TOTABSCORE_TH);
		double contentscore = contentScores[contentScores.length-1][0];
		if (contentscore>=_thres)
			//ClassifierDatum result = new ClassifierDatum(url, new String[1],new String[1][2], 0.0, 0.0);
			return result;
		else
			return null;
	}


	public static ClassifierDatum classifyText(Double[][] scores1, Double[][] scores2,
			Double[][] scores3, Double[][] scores4, double thr1, double thr2, 
			double thr3, String[] classes, String url) {
		//get the arrays with scores for each location, the thresholds and the sub-classes
		//Boolean pass=false;
		double norm_score=0; 
		Double total_score=0.0;
		Double total_relscore = 0.0;
		//add the scores
		Double[][] sums= scores1;
		for (int ii=0;ii<scores1.length;ii++){
			sums[ii][0] += scores2[ii][0]+ scores3[ii][0]+scores4[ii][0];
			sums[ii][1] += scores2[ii][1]+ scores3[ii][1]+scores4[ii][1];
		}
		//print the sums of the scores for checking 
		//System.out.println("SUMS");
		//for (int ii=0;ii<scores1.length;ii++){
		//	System.out.println(sums[ii][0]+"        "+ sums[ii][1]);
		//}
		//if the total absolute and relative scores are over the corresponding thresholds
		//calculate a normalized score per subclass and compare it with another threshold
		ArrayList<String> subclasses = new ArrayList<String>();
		ArrayList<Double[][]> subscores = new ArrayList<Double[][]>(); 
		if (sums[sums.length-1][0]>thr1 && sums[sums.length-1][1]>thr2){
			//pass=true;
			for (int ii=0;ii<scores1.length-1;ii++){
				//norm_score =100 *sums[ii][1]/sums[scores1.length-1][1];
				norm_score = sums[ii][1]/sums[scores1.length-1][1];
				if (norm_score>thr3){
					subclasses.add(classes[ii]);
					//System.out.println("Text is in the subclass " + classes[ii]+".");
					total_score += sums[ii][0];
					total_relscore += sums[ii][1];
					subscores.add(new Double[][]{new Double[] {sums[ii][0],sums[ii][1]}});
				}
			}
		}
		String[] subclasses1=new String[subclasses.size()];
		for (int i=0;i<subclasses.size();i++){
			subclasses1[i]= subclasses.get(i);
		}
		Double[][] subscores1 = new Double[subscores.size()][2];
		for (int i=0;i<subscores.size();i++){
			Double[][] temp=subscores.get(i);
			subscores1[i][0] = temp[0][0];
			subscores1[i][1] = temp[0][1];
		}
		//System.out.println("The total score is "+total_score);
		ClassifierDatum result = new ClassifierDatum(url, subclasses1,subscores1, total_score, total_relscore);
		return result;
	}


	/*public static Double[][] rankText(String str, double w,ArrayList<String[]> topic, String[] classes){
		//str: String to classify,  topic: list of triplets,  classes:list of classes
		//Returns an array of doubles with 2 columns. 
		//The number of rows are as many as the subclasses plus one (the "total"). 
		//The first contains the absolute score per subclass, while the second relative scores, 
		//i.e. the absolute scores divided by the number of words. 



		//stem the input str
		ArrayList<String> stems = null;
		try {
			stems = TopicTools.analyze(str, langIdentified);
		} catch (IOException e) {
			LOGGER.warn(e.getMessage());
			return null;
		}

		//concatenate stems 
		str="";
    	for (String s:stems){ str=str.concat(" "+s);}
    	str = str.trim();
    	//System.out.println(str);
		//word counting
		StringTokenizer st = new StringTokenizer(str);	
		double words_num = 0;// String ss="";
		words_num = st.countTokens();
		//while (st.hasMoreTokens()) { ss= st.nextToken();words_num++;}
		//initialization of scores array
		Double[][] scores = new Double[classes.length+1][2];
		for (int ii=0;ii<classes.length;ii++){
			scores[ii][0] = 0.0;	scores[ii][1] = 0.0;
		}		
		scores[classes.length][0]=0.0;  scores[classes.length][1]=0.0;
		if (words_num==0) return scores;
		//lists with the found terms
		ArrayList<String> termfound=new ArrayList<String>(); 
		ArrayList<String> termfoundid=new ArrayList<String>();
		//calculate the initial scores based on the occurrences of each term of the topic 
		//in the str and the weight of the term
		String[] tempstr;		String term, term_class;
		double weight=0, matches, term_score; 
		int index;
		for (int ii=0;ii<topic.size();ii++){ //for each row of the topic
			//get the term
			tempstr=topic.get(ii);
			term = tempstr[1];
			matches=0;
			term_score=0;
			//find term in text
			Pattern pattern = Pattern.compile(" "+term+" ");	
			Matcher matcher = pattern.matcher(" "+str+" ");
			//list with positions of a found term
			ArrayList<String> termpos=new ArrayList<String>(); 
			while (matcher.find()) {
				//get the position of a found term
				termpos.add(Integer.toString(matcher.start()));
				matches++;
			}
			if (matches>0){
				//add found term
				termfound.add(term);
				//add id of the found term
				termfoundid.add(Integer.toString(ii+1));
				//get the weight of the term
				weight=Double.parseDouble(tempstr[0]);
				term_score = weight*matches;
				//get the subclass of the term
				term_class = tempstr[2];
				//add the "contribution" of the term to scores array, in the row 
				//which corresponds to the subclass  
				index = Arrays.binarySearch(classes, term_class);
				scores[index][0] =scores[index][0]+term_score;
				//print found terms and their positions for checking
				//System.out.println("Term :"+ term +" with termid:"+ (ii+1) +" in posistions: " +termpos);
			}
		}
		//calculate scores with the weight of the text (the location weight) 
		//and the relative scores
		for (int ii=0;ii<classes.length;ii++){
			scores[ii][0] = scores[ii][0]*w;
			scores[ii][1] = scores[ii][0]/words_num; 
			scores[classes.length][0] =scores[ii][0]+scores[classes.length][0];
			scores[classes.length][1] =scores[ii][1]+scores[classes.length][1];
		}
		//print results for checking
		//System.out.println("Num of words: "+words_num +" for location with weigth "+ w);
		//for (int ii=0;ii<scores.length;ii++) {
		//	System.out.println(scores[ii][0]+"    "+scores[ii][1]);
		//}
		return scores;
	}*/
	public static Double[][] rankText(String str, double w,ArrayList<String[]> topic, String[] classes, boolean isContent){
		//str: String to classify,  topic: list of triplets,  classes:list of classes
		//Returns an array of doubles with 2 columns. 
		//The number of rows are as many as the subclasses plus one (the "total"). 
		//The first contains the absolute score per subclass, while the second relative scores, 
		//i.e. the absolute scores divided by the number of words. 
		int uniqueTermsFound = 0;

		//stem the input str
		ArrayList<String> stems =new ArrayList<String>();
		try {
			stems = TopicTools.analyze(str, langIdentified);
		} catch (IOException e) {
			LOGGER.warn(e.getMessage());
			return null;
		}
		//concatenate stems 
		str="";
		for (String s:stems){ str+=" "+s;}
		str = str.trim();
		
		//System.out.println(str);
		//word counting
		StringTokenizer st = new StringTokenizer(str);	
		double words_num=st.countTokens();
		//double words_num = 0; String ss="";
		//while (st.hasMoreTokens()) { ss= st.nextToken();words_num++;}
		//initialization of scores array
		Double[][] scores = new Double[classes.length+1][2];
		for (int ii=0;ii<classes.length;ii++){
			scores[ii][0] = 0.0;	scores[ii][1] = 0.0;
		}		
		scores[classes.length][0]=0.0;  scores[classes.length][1]=0.0;
		if (words_num==0.0) return scores;
		//lists with the found terms
		//ArrayList<String> termfound=new ArrayList<String>(); 
		//ArrayList<String> termfoundid=new ArrayList<String>();
		//calculate the initial scores based on the occurrences of each term of the topic 
		//in the str and the weight of the term
		String[] tempstr = new String[1];		String term, term_class;
		double weight=0, matches, term_score; 
		int index;
		for (int ii=0;ii<topic.size();ii++){ //for each row of the topic
			//get the term
			tempstr=topic.get(ii);
			term = tempstr[1];
			matches=0;
			term_score=0;
			//find term in text
			Pattern pattern = Pattern.compile(" "+term+" ");	
			Matcher matcher = pattern.matcher(" "+str+" ");
			//list with positions of a found term
			ArrayList<String> termpos=new ArrayList<String>(); 
			while (matcher.find()) {
				//get the position of a found term
				termpos.add(Integer.toString(matcher.start()));
				matches++;
			}
			if (matches>0){
				uniqueTermsFound++;
				//add found term
				//termfound.add(term);
				//add id of the found term
				//termfoundid.add(Integer.toString(ii+1));
				//get the weight of the term
				weight=Double.parseDouble(tempstr[0]);
				term_score = weight*matches;
				//get the subclass of the term
				term_class = tempstr[2];
				if (term_class.contains(";")){
					String[] term_classes = term_class.split(";");
					int term_classes_amount = term_classes.length;
					for (int mm=0;mm<term_classes_amount;mm++){
						index = Arrays.binarySearch(classes, term_classes[mm]);
						scores[index][0] =scores[index][0]+term_score/term_classes_amount;
					}
				}
				else{
					index = Arrays.binarySearch(classes, term_class);
					scores[index][0] =scores[index][0]+term_score;
				}
				//add the "contribution" of the term to scores array, in the row 
				//which corresponds to the subclass  
				//index = Arrays.binarySearch(classes, term_class);
				//scores[index][0] =scores[index][0]+term_score;
				//print found terms and their positions for checking				
			}
		}
		//Check if found terms in the content are equal or above the predefined threshold
		if (isContent) {
			if (uniqueTermsFound<_min_uniq_terms && _min_uniq_terms>0) {
				for (int ii=0;ii<classes.length;ii++){
					scores[ii][0] = 0.0;	scores[ii][1] = 0.0;
				}
				return scores;
			}
		}
		//calculate scores with the weight of the text (the location weight) 
		//and the relative scores
		for (int ii=0;ii<classes.length;ii++){
			scores[ii][0] = scores[ii][0]*w;
			scores[ii][1] = scores[ii][0]/words_num; 
			scores[classes.length][0] =scores[ii][0]+scores[classes.length][0];
			scores[classes.length][1] =scores[ii][1]+scores[classes.length][1];			
		}		
		return scores;
	}


	public double rankLink(String text1) {
		double score = 0, weight=0; int matches=0;
		String[] tempstr = null;		
		String term;
		String text = text1 .trim();
		ArrayList<String> stems =new ArrayList<String>();
		try {
			stems = TopicTools.analyze(text, _targetLanguage);
		} catch (IOException e) {
			e.printStackTrace();
		}
		text="";
		for (String s:stems){ text=text.concat(" "+s);}
		text = text.trim();
		for (int ii=0;ii<_topic.size();ii++){ 
			tempstr=_topic.get(ii);
			term = tempstr[1];
			matches=0;
			Pattern pattern = Pattern.compile(" "+term+" ");

			Matcher matcher = pattern.matcher(" "+text+" ");
			while (matcher.find()) {

				matches++;
			}
			if (matches>0){
				weight=Double.parseDouble(tempstr[0]);
				score += weight*matches;
				//System.out.println("FoundTerm :"+ term );
			}
		}
		return score;
	}


	private String cleanContent(String content){
		String result = "";
		String REGEX = "<text>.*</text>";
		String REPLACE = " ";
		Pattern p = Pattern.compile(REGEX);
		Matcher m = p.matcher(content);
		String text = "";
		while (m.find()){
			text = m.group();
			text = text.replaceAll("</?text>", REPLACE);
			result = result.concat(text);
		}
		return result;
	}
	public ArrayList<String[]> getTopic(){
		return _topic;
	}
	public int getMaxDepth() { return _max_depth;}
}
