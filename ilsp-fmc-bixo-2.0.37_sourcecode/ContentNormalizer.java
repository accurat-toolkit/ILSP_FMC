package gr.ilsp.fmc.utils;


import java.util.HashMap;


public class ContentNormalizer {
	
	private static final HashMap<String,String> invalidChars = new HashMap<String,String>(){
	    /**
		 * 
		 */
		private static final long serialVersionUID = -7208860988679686271L;

		{ 
			put("\\uFEFF", " "); //ZERO WIDTH NO-BREAK SPACE
	        put("\\u00a0", " "); //NO BREAK SPACE
	        put("\\u200E"," "); //LEFT-TO-RIGHT MARK
	        put("\\u0097", "-"); //END OF GUARDED AREA
	        put("\\u0092", "’"); //APOSTROPHE
	        put("\\u0313","’"); //COMBINING COMMA ABOVE
	        put("\\u0094", "”"); //CLOSE DOUBLE QUOTE
	        put("\\u0093", "“"); //OPEN DOUBLE QUOTE
	        put("\\u0095", "•"); 
	        put("\\u0096", "-");
	        put("\\u0081", " ");
	        put("\\u202f", " "); //NARROW NO-BREAK SPACE
	        put("\\u2206", "Δ"); //INCREMENT
	        put("\\u02bc", "’"); //MODIFIER LETTER APOSTROPHE
	        put("\\u003e", ">"); //GREATER-THAN SIGN
	        put("\\uFFFD",""); //REPLACEMENT CHARACTER Specials
	        put("\\uF0D8",""); //INVALID UNICODE CHARACTER
	        put("\\uF02D",""); //INVALID UNICODE CHARACTER
	        put("\\uF0FC",""); //INVALID UNICODE CHARACTER
	        put("\\uF034",""); //INVALID UNICODE CHARACTER
	        put("\\uF076",""); //INVALID UNICODE CHARACTER
	        put("\\uF0BC",""); //INVALID UNICODE CHARACTER
	        put("\\uF06C",""); //INVALID UNICODE CHARACTER
	        put("\\uF0E8",""); //INVALID UNICODE CHARACTER
	        put("\\uF0B7",""); //INVALID UNICODE CHARACTER
	        put("\\uF0A7",""); //INVALID UNICODE CHARACTER
	        put("\\uF0FB",""); //INVALID UNICODE CHARACTER
	        put("\\uF06E",""); //INVALID UNICODE CHARACTER
	        put("\\uF0F1",""); //INVALID UNICODE CHARACTER
	        put("\\uF075",""); //INVALID UNICODE CHARACTER
	        put("\\u2126","Ω"); //OHM SIGN
	        put("\\u25B6","►"); //BLACK RIGHT-POINTING TRIANGLE
	        put("\\u200F"," "); //RIGHT-TO-LEFT MARK
	        put("\\u0080","€"); //RIGHT-TO-LEFT MARK
	        
	    	
	    }
	};
	
	
	public static String normalizeText(String text){
		for (String s:invalidChars.keySet()){
			text = text.replaceAll(s, invalidChars.get(s));
		}
		text = text.replaceAll("(\\s){2,}", " ");
		
		text = text.replaceAll("<text> ", "<text>");
		text = text.replaceAll("<boiler> ", "<boiler>");
		text = text.replaceAll(" </text>", "</text>");
		text = text.replaceAll(" </boiler> ", "</boiler>");
		text = text.replaceAll("<text></text>", "");
		text = text.replaceAll("<boiler></boiler>", "");
				
		text = text.replaceAll("(\n){2,}","\n");
		return text;
	}

	
	
}
