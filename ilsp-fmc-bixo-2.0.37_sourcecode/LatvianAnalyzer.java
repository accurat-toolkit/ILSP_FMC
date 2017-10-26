package gr.ilsp.fmc.utils;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LatvianAnalyzer {
	private static Pattern main1 = Pattern.compile("^([^a-zāčēģīķļņšūžA-ZĀČĒĢĪĶĻŅŠŪŽ]*)(.*)");
	private static Pattern main2 = Pattern.compile("^([a-zāčēģīķļņšūžA-ZĀČĒĢĪĶĻŅŠŪŽ]+)(.*)");

	private static Pattern p1 =  Pattern.compile("^(ārā|cik|kad|maz|pus|rīt|sen|šad|šur|tur|žēl|" +
			"kur|jau|tad|vēl|tik|pie|pēc|gar|par|pār|bez|aiz|zem|dēļ|lai|vai|arī|gan|" +
	"bet|jeb|būt|esi|būs|kas|kam|kur)$");

	private static Pattern p2 = Pattern.compile("(amākajiem|ākumiņiem|āmākajiem|ošākajiem)$");

	private static Pattern p3 = Pattern.compile("(amākajai|amākajam|amākajām|amākajās|amākajos|ākumiņam|" +
			"ākumiņos|ākumiņus|āmākajai|āmākajam|āmākajām|āmākajās|āmākajos|īsieties|" +
	"ošākajai|ošākajam|ošākajām|ošākajās|ošākajos|tājiņiem|tākajiem)$");

	private static Pattern p4 = Pattern.compile("(amajiem|amākais|amākajā|amākiem|ākajiem|ākumiem|ākumiņa|" +
			"ākumiņā|ākumiņi|ākumiņš|ākumiņu|āmajiem|āmākais|āmākajā|āmākiem|ējiņiem|īsimies|" +
			"īsities|īšoties|ošajiem|ošākais|ošākajā|ošākiem|sieties|šaniņai|šaniņas|šaniņām|" +
			"šaniņās|šaniņos|tājiņai|tājiņam|tājiņas|tājiņām|tājiņās|tājiņos|tājiņus|tākajai|" +
	"tākajam|tākajām|tākajās|tākajos|umiņiem|ušajiem|utiņiem)$");

	private static Pattern p5 = Pattern.compile("(amajai|amajam|amajām|amajās|amajos|amākai|amākam|amākas|amākām|" +
			"amākās|amākie|amākos|amākus|ākajai|ākajam|ākajām|ākajās|ākajos|ākumam|ākumiņ|ākumos|" +
			"ākumus|āmajai|āmajam|āmajām|āmajās|āmajos|āmākai|āmākam|āmākas|āmākām|āmākās|āmākie|" +
			"āmākos|āmākus|damies|ējiņai|ējiņam|ējiņas|ējiņām|ējiņās|ējiņos|ējiņus|ieties|ošajai|" +
			"ošajam|ošajām|ošajās|ošajos|ošākai|ošākam|ošākas|ošākām|ošākās|ošākie|ošākos|ošākus|" +
			"simies|sities|sniņai|sniņas|sniņām|sniņās|šaniņa|šaniņā|šaniņu|šoties|tajiem|tājiem|" +
			"tājiņa|tājiņā|tājiņi|tājiņš|tājiņu|tākais|tākajā|tākiem|tiņiem|umiņam|umiņos|umiņus|" +
	"ušajai|ušajam|ušajām|ušajās|ušajos|utiņam|utiņos|utiņus)$");

	private static Pattern p6 = Pattern.compile("(ajiem|amais|amajā|amāka|amākā|amāki|amāko|amāks|amāku|amiem|amies|" +
			"aties|ākais|ākajā|ākiem|ākuma|ākumā|ākumi|ākums|ākumu|āmais|āmajā|āmāka|āmākā|āmāki|" +
			"āmāko|āmāks|āmāku|āmiem|āmies|āties|damas|damās|ējais|ējiem|ējiņa|ējiņā|ējiņi|ējiņš|" +
			"ējiņu|iņiem|īsies|īsiet|īšiem|ošais|ošajā|ošāka|ošākā|ošāki|ošāko|ošāks|ošāku|ošiem|" +
			"ošies|oties|sniņa|sniņā|sniņu|šanai|šanas|šanām|šanās|šanos|tajai|tajam|tajām|tajās|" +
			"tajos|tājai|tājam|tājas|tājām|tājās|tājiņ|tājos|tājus|tākai|tākam|tākas|tākām|tākās|" +
			"tākie|tākos|tākus|tiņai|tiņam|tiņas|tiņām|tiņās|tiņos|tiņus|umiem|umiņa|umiņā|umiņi|" +
	"umiņš|umiņu|usies|ušais|ušajā|ušiem|ušies|utiņa|utiņā|utiņi|utiņš|utiņu)$");

	private static Pattern p7 = Pattern.compile("(ajai|ajam|ajām|ajās|ajos|amai|amam|amas|amām|amās|amie|amos|amus|" +
			"anīs|ākai|ākam|ākas|ākām|ākās|ākie|ākos|ākus|āmai|āmam|āmas|āmām|āmās|āmie|āmos|āmus|" +
			"dama|dami|dams|ējai|ējam|ējas|ējām|ējās|ējie|ējiņ|ējos|ējus|inīs|iņai|iņam|iņas|iņām|" +
			"iņās|iņos|iņus|īsim|īsit|īšos|īšot|īšus|ītei|ītem|ītes|ītēm|ītēs|ītim|ītis|jiem|ošai|" +
			"ošam|ošas|ošām|ošās|ošie|ošos|ošus|sies|siet|sniņ|šana|šanā|šanu|tais|tajā|tāja|tājā|" +
			"tāji|tājs|tāju|tāka|tākā|tāki|tāko|tāks|tāku|tiem|ties|tiņa|tiņā|tiņi|tiņš|tiņu|umam|" +
	"umiņ|umos|umus|ušai|ušam|ušas|ušām|ušās|ušie|ušos|ušus|utiņ)$");

	private static Pattern p8 = Pattern.compile("(ais|ajā|ama|amā|ami|amo|ams|amu|anī|āka|ākā|āki|āko|āks|āku|āma|" +
			"āmā|āmi|āmo|āms|āmu|ēja|ējā|ēji|ējo|ējs|ēju|iem|ies|iet|inī|iņa|iņā|iņi|iņš|iņu|īsi|" +
			"īša|īši|īšu|īte|ītē|īti|ītī|jām|jās|jos|oša|ošā|oši|ošo|ošs|ošu|sim|sit|šos|šot|tai|" +
	"tam|tas|tāj|tām|tās|tie|tiņ|tos|tus|uma|umā|umi|ums|umu|usi|usī|uša|ušā|uši|ušo|ušu)$");

	private static Pattern p9 = Pattern.compile("(ai|am|as|at|āk|ām|ās|āt|ei|em|es|ēj|ēm|ēs|ie|ij|im|iņ|is|īm|īs|īt|" +
	"ju|mu|os|ot|si|šu|ta|tā|ti|to|ts|tu|um|ur|us)$");

	private static Pattern p10 = Pattern.compile("(a|ā|e|ē|i|ī|m|o|s|š|t|u|ū)$");

	public static ArrayList<String> analyze(String text){
		text = text.toLowerCase();		

		ArrayList<String> txtBlocks = getBlocks(text);
		Matcher m2 = null;
		String token = null, token2 = null;
		ArrayList<String> stems = new ArrayList<String>();
		Matcher matcher = null;
		for (String s:txtBlocks){
			matcher = main1.matcher(s);

			while (true){
				matcher.find();
				token = matcher.group(2);			
				m2 = main2.matcher(token);
				if (m2.find()){	
					token2 = m2.group(1);
					stems.add(stem(token2));
					s = m2.group(2);
					matcher = main1.matcher(s);
				} else break;			
			}
		}
		return stems;
	}

	private static ArrayList<String> getBlocks(String text) {
		ArrayList<String> blocks = new ArrayList<String>();
		String stringBlock = "";
		int index = 0;
		while (text.length()>200){
			index = text.indexOf(" ",50);
			if (index==-1) index = text.length();
			stringBlock = text.substring(0,index);
			blocks.add(stringBlock);
			text = text.substring(stringBlock.length());
		}
		blocks.add(text);
		return blocks;
	}

	public static  String stem(String s){
		if (s.length()<2) return s;


		Matcher matcher = p1.matcher(s);

		if (matcher.find()) return s;

		matcher = p2.matcher(s);
		if (matcher.find()){
			if (s.length()>=11)
				return s.substring(0,s.length()-9);
		}


		matcher = p3.matcher(s);
		if (matcher.find()){
			if (s.length()>=10)
				return s.substring(0,s.length()-8);
		}


		matcher = p4.matcher(s);
		if (matcher.find()){
			if (s.length()>=9)
				return s.substring(0,s.length()-7);
		}


		matcher = p5.matcher(s);
		if (matcher.find()){
			if (s.length()>=8)
				return s.substring(0,s.length()-6);
		}


		matcher = p6.matcher(s);
		if (matcher.find()){
			if (s.length()>=7)
				return s.substring(0,s.length()-5);
		}


		matcher = p7.matcher(s);
		if (matcher.find()){
			if (s.length()>=6)
				return s.substring(0,s.length()-4);
		}


		matcher = p8.matcher(s);
		if (matcher.find()){
			if (s.length()>=5)
				return s.substring(0,s.length()-3);
		}


		matcher = p9.matcher(s);
		if (matcher.find()){
			if (s.length()>=4)
				return s.substring(0,s.length()-2);
		}


		matcher = p10.matcher(s);
		if (matcher.find()){
			if (s.length()>=3)
				return s.substring(0,s.length()-1);
		}

		return s;
	}



}
