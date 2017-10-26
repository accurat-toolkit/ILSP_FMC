package gr.ilsp.fmc.datums;





import bixo.datum.UrlDatum;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class ClassifierDatum extends UrlDatum {
    
	public static final String SUBCLASSES = fieldName(ClassifierDatum.class,"subclasses");
	public static final String SUBSCORES = fieldName(ClassifierDatum.class,"subscores");
	public static final String TOTABSCORE = fieldName(ClassifierDatum.class,"totabscore");
	public static final String TOTRELSCORE = fieldName(ClassifierDatum.class,"totrelscore");
	
    public static final Fields FIELDS = new Fields(SUBCLASSES, SUBSCORES, TOTABSCORE, 
    		TOTRELSCORE).append(getSuperFields(ClassifierDatum.class));

    /**
     * No argument constructor for use with FutureTask
     */
    public ClassifierDatum() {
        super(FIELDS);
    }
    
    public ClassifierDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry, FIELDS);
    }
    
    public ClassifierDatum(String url, String[] subclasses, Double[][] subscores, Double totabscore, Double totrelscore) {
        super(FIELDS);
        
        setUrl(url);
        setSubClasses(subclasses);
        setSubScores(subscores);
        setTotAbScore(totabscore);
        setTotRelScore(totrelscore);        
    }

    public String[] getSubClasses() {
    	Tuple tup = (Tuple)_tupleEntry.get(SUBCLASSES);    	
        String[] subclasses = new String[tup.size()];
        
        for (int i = 0; i < tup.size(); i++) {            
            subclasses[i] = (String) tup.get(i);
        }
        
        return subclasses;
    }
    public void setSubClasses(String[] subclasses) {
        _tupleEntry.set(SUBCLASSES, convertSubClassesToTuple(subclasses));
    }
    private Tuple convertSubClassesToTuple(String[] subclasses) {
        Tuple tuple = new Tuple();
        for (String subclass : subclasses) {
            tuple.add(subclass);                        
        }        
        return tuple;
    }
    public void setSubScores(Double[][] subscores) {
        _tupleEntry.set(SUBSCORES, convertSubScoresToTuple(subscores));
    }
    private Tuple convertSubScoresToTuple(Double[][] subscores) {
        Tuple tuple = new Tuple();
        int len = subscores.length;
        for (int i = 0; i<len; i++) {
            tuple.add(subscores[i][0]);
            tuple.add(subscores[i][1]);
        }        
        return tuple;
    }
    public Double[][] getSubScores() {
    	Tuple tup = (Tuple)_tupleEntry.get(SUBSCORES);    	
    	Double[][] subscores = new Double[tup.size()/2][2];
        
        for (int i = 0; i < tup.size()/2; i++) {
            int tupleOffset = i * 2;
            subscores[i][0] = (Double) tup.get(tupleOffset);
            subscores[i][1] = (Double) tup.get(tupleOffset+1);
        }        
        return subscores;
    }
    
    public void setTotAbScore(Double totabscore) {
        _tupleEntry.set(TOTABSCORE, totabscore);
    }

    public Double getTotAbScore() {
        return _tupleEntry.getDouble(TOTABSCORE);
    }
    
    public void setTotRelScore(Double totrelscore) {
        _tupleEntry.set(TOTRELSCORE, totrelscore);
    }

    public Double getTotRelScore() {
        return _tupleEntry.getDouble(TOTRELSCORE);
    }       
    public String toString() {
        return getUrl() + "\t" + getTotAbScore() + "\t" + getTotRelScore();
    }
}
