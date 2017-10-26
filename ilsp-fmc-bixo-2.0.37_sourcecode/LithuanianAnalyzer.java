package gr.ilsp.fmc.utils;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//Mārcis: We renamed the class:
public class LithuanianAnalyzer {
	//Mārcis: We replaced the valid symbol patterns:
	private static Pattern main1 = Pattern.compile("^([^a-ząčęėįšųūžA-ZĄČĘĖĮŠŲŪŽ]*)(.*)");
	private static Pattern main2 = Pattern.compile("^([a-ząčęėįšųūžA-ZĄČĘĖĮŠŲŪŽ]+)(.*)");

	//We replaced the "do not stem" list:
	private static Pattern p1 =  Pattern.compile("^(aukščiau|antrapus|aplinkui|įstrigai|priešais|įstrižai|kadangi|tarytum|taipogi|įkandin|kitapus|apsukui|kiaurai|išilgai|skersai|tačiau|tiktai|nebent|tartum|nekaip|būtent|destis|linkui|vietoj|netoli|pusiau|toliau|vidury|žemiau|anapus|abipus|šiapus|abišal|aplink|įkypai|paskui|paskum|viršum|vardan|betgi|taigi|jeigu|idant|tegul|tarsi|užuot|beigi|tolei|vėlgi|visgi|nesgi|užtat|todėl|šalia|dėlei|greta|pasak|pirma|viduj|apsuk|pagal|palei|prieš|sulig|vidur|dėlgi|arba|kada|ligi|nors|tegu|kaip|negu|kuom|oigi|argi|tiek|prie|dėka|link|anot|arti|ligi|tarp|virš|apie|ties|pirm|bei|nei|bet|ogi|tik|tai|tad|kai|iki|lig|kol|vos|nes|jei|lyg|kad|jog|tol|čia|juo|kuo|tuo|ant|dėl|lig|nuo|pas|per|pro|iki)$");

	//Mārcis: We inserted longer endings as p2_#:
	
	private static Pattern p2_5 = Pattern.compile("(ėlesniuosiuose|siančiuosiuose|šiančiuosiuose)$");
	
	private static Pattern p2_4 = Pattern.compile("(ėlesniaisiais|ėlesniosiomis|iančiuosiuose|iausiuosiuose|siančiaisiais|siančiosiomis|šiančiaisiais|šiančiosiomis)$");
	
	private static Pattern p2_3 = Pattern.compile("(ančiuosiuose|ausiuosiuose|ėlesniesiems|ėlesniosioms|ėlesniosiose|ėlesniuosius|esniuosiuose|iančiaisiais|iančiosiomis|iausiaisiais|iausiosiomis|inčiuosiuose|iusiuosiuose|siančiosioms|siančiosiose|siančiuosius|siantiesiems|šiančiosioms|šiančiosiose|šiančiuosius|šiantiesiems)$");
	
	private static Pattern p2_2 = Pattern.compile("(ančiaisiais|ančiosiomis|ausiaisiais|ausiosiomis|ėlesniajame|ėlesniąsias|ėlesniojoje|ėlesniosios|esniaisiais|esniosiomis|iamuosiuose|iančiosioms|iančiosiose|iančiuosius|iantiesiems|iausiesiems|iausiosioms|iausiosiose|iausiuosius|inčiaisiais|inčiosiomis|iusiaisiais|iusiosiomis|siančiajame|siančiąsias|siančiojoje|siančiosios|šiančiajame|šiančiąsias|šiančiojoje|šiančiosios|usiuosiuose)$");
	
	private static Pattern p2_1 = Pattern.compile("(amuosiuose|ančiosioms|ančiosiose|ančiuosius|antiesiems|ausiesiems|ausiosioms|ausiosiose|ausiuosius|davusiomis|davusiuose|ėlesniajai|ėlesniajam|ėlesniuoju|ėlesniuose|esniesiems|esniosioms|esniosiose|esniuosius|iamaisiais|iamiesiems|iamosiomis|iančiajame|iančiąsias|iančiojoje|iančiosios|iausiajame|iausiąsias|iausiojoje|iausiosios|imuosiuose|inčiosioms|inčiosiose|inčiuosius|intiesiems|iusiesiems|iusiosioms|iusiosiose|iusiuosius|omuosiuose|siančiajai|siančiajam|siančiomis|siančiuoju|siančiuose|šiančiajai|šiančiajam|šiančiomis|šiančiuoju|šiančiuose|usiaisiais|usiosiomis)$");
	
	//Mārcis: END OF ADDITIONAL CODE.
	
	//Mārcis: We replaced the existing ending regular expressions:
	private static Pattern p2 = Pattern.compile("(amaisiais|amiesiems|amosiomis|ančiajame|ančiąsias|ančiojoje|ančiosios|ausiajame|ausiąsias|ausiojoje|ausiosios|davusiais|davusiame|davusiems|davusioje|davusioms|davusiose|davusiosi|ėlesnėmis|ėlesniais|ėlesniąja|ėlesniąją|ėlesniame|ėlesnieji|ėlesniems|ėlesnioji|ėlesniojo|ėlesniųjų|ėlesnysis|esniajame|esniąsias|esniojoje|esniosios|iamosioms|iamosiose|iamuosius|iančiajai|iančiajam|iančiomis|iančiuoju|iančiuose|iausiajai|iausiajam|iausiasis|iausiomis|iausiuoju|iausiuose|imaisiais|imiesiems|imosiomis|inčiajame|inčiąsias|inčiojoje|inčiosios|iuosiuose|iusiajame|iusiąsias|iusiojoje|iusiosios|omaisiais|omiesiems|omosiomis|siančiais|siančiąja|siančiąją|siančiame|siančioje|siančioji|siančiojo|siančioms|siančiose|siančiosi|siančiųjų|siančiuos|siantieji|siantiems|siantysis|šiančiais|šiančiąja|šiančiąją|šiančiame|šiančioje|šiančioji|šiančiojo|šiančioms|šiančiose|šiančiosi|šiančiųjų|šiančiuos|šiantieji|šiantiems|šiantysis|tuosiuose|usiesiems|usiosioms|usiosiose|usiuosius)$");

	private static Pattern p3 = Pattern.compile("(amosioms|amosiose|amuosius|ančiajai|ančiajam|ančiomis|ančiuoju|ančiuose|ausiajai|ausiajam|ausiasis|ausiomis|ausiuoju|ausiuose|davusiai|davusiam|davusias|davusios|davusius|ėlesnėje|ėlesnėms|ėlesnėse|ėlesniam|ėlesnįjį|ėlesnius|esniajai|esniajam|esniuoju|esniuose|iaisiais|iamajame|iamąsias|iamojoje|iamosios|iančiais|iančiąja|iančiąją|iančiame|iančioje|iančioji|iančiojo|iančioms|iančiose|iančiųjų|iančiuos|iantieji|iantiems|iantysis|iausiais|iausiąja|iausiąją|iausiąjį|iausiame|iausieji|iausiems|iausioje|iausioji|iausiojo|iausioms|iausiose|iausiųjų|iemdviem|imosioms|imosiose|imuosius|inčiajai|inčiajam|inčiomis|inčiuoju|inčiuose|iosiomis|iuosiuos|iusiajai|iusiajam|iusiomis|iusiuoju|iusiuose|omosioms|omosiose|omuosius|siančiai|siančiam|siančias|siančioj|siančiom|siančios|siančius|siantiem|siantįjį|šiančiai|šiančiam|šiančias|šiančioj|šiančiom|šiančios|šiančius|šiantiem|šiantįjį|taisiais|tiesiems|tosiomis|uosiuose|usiajame|usiąsias|usiojoje|usiosios|viejuose)$");

	private static Pattern p4 = Pattern.compile("(aisiais|amajame|amąsias|amojoje|amosios|ančiais|ančiąja|ančiąją|ančiame|ančioje|ančioji|ančiojo|ančioms|ančiose|ančiųjų|ančiuos|antieji|antiems|antysis|ausiais|ausiąja|ausiąją|ausiąjį|ausiame|ausieji|ausiems|ausioje|ausioji|ausiojo|ausioms|ausiose|ausiųjų|čiausių|damasis|damiesi|davaisi|davausi|davęsis|davomės|davotės|davusia|davusią|davusio|davusis|davusiu|davusių|ėlesnei|ėlesnes|ėlesnės|ėlesnio|ėlesnis|ėlesniu|ėlesnių|emdviem|esnėmis|esniais|esniąja|esniąją|esniame|esnieji|esniems|esnioji|esniojo|esniųjų|esnysis|iamajai|iamajam|iamasis|iamieji|iamiems|iamomis|iamuoju|iamuose|iančiai|iančiam|iančias|iančioj|iančiom|iančios|iančius|iantiem|iantįjį|iausiai|iausiam|iausias|iausios|iausius|iedviem|iejomis|iejuose|iesiems|imaisis|imajame|imamsis|imąsias|imojoje|imosios|inčiais|inčiąja|inčiąją|inčiame|inčioje|inčioji|inčiojo|inčioms|inčiose|inčiųjų|inčiuos|intieji|intiems|intysis|iosioms|iosiose|iuosius|iusiais|iusiąja|iusiąją|iusiame|iusieji|iusiems|iusioje|iusioji|iusiojo|iusioms|iusiose|iusiosi|iusiųjų|iusysis|omajame|omąsias|omdviem|omojoje|omosios|osiomis|siančia|siančią|siančio|siančiu|siančių|siantis|siantys|simiems|simomis|simuose|šiančia|šiančią|šiančio|šiančiu|šiančių|šiantis|šiantys|šimiems|šimomis|šimuose|tiniems|tinomis|tinuose|tosioms|tosiose|tumeisi|tumėmės|tumėtės|tuosius|ųdviejų|uosiuos|usiajai|usiajam|usiomis|usiuoju|usiuose|viejose|ymaisis|ymamsis)$");

	private static Pattern p5 = Pattern.compile("(amajai|amajam|amasis|amieji|amiems|amomis|amuoju|amuose|ančiai|ančiam|ančias|ančioj|ančiom|ančios|ančius|antiem|antįjį|ausiai|ausiam|ausias|ausios|ausius|čiausi|čiomis|čiuose|damasi|damosi|davais|davaus|davęsi|davome|davosi|davote|davusi|davusį|edviem|ėlesne|ėlesnė|ėlesnę|ėlesni|ėlesnį|esnėje|esnėms|esnėse|esniam|esnįjį|esnius|iajame|iamais|iamąja|iamąją|iamąjį|iamame|iamasi|iamoje|iamoji|iamojo|iamoms|iamose|iamųjų|iančia|iančią|iančio|iančiu|iančių|iantis|iantys|iąsias|iausia|iausią|iausio|iausiu|iausių|iejose|iesiem|ijuose|imaisi|imajai|imajam|imasis|imieji|imiems|imomis|imuisi|imuoju|imuose|imusis|inčiai|inčiam|inčias|inčioj|inčiom|inčios|inčius|intiem|intįjį|iojoje|iosiom|iosios|iusiai|iusiam|iusias|iusiem|iusįjį|iusioj|iusiom|iusios|iusius|odviem|omajai|omajam|omasis|omieji|omiems|omomis|omuoju|omuose|osioms|osiose|sianti|siantį|siąsis|simais|simame|simoje|simoms|simose|siuosi|šianti|šiantį|šiąsis|šimais|šimame|šimoje|šimoms|šimose|šiuosi|tajame|tąsias|tinais|tiname|tiniem|tinoje|tinoms|tinose|tinuos|tojoje|tosios|tumeis|tumėme|tumėte|uosius|usiais|usiąja|usiąją|usiame|usieji|usiems|usioje|usioji|usiojo|usioms|usiose|usiosi|usiųjų|usysis|ymaisi|ymasis|ymuisi|ymuose|ymusis)$");

	private static Pattern p6 = Pattern.compile("(ajame|amais|amąja|amąją|amąjį|amame|amasi|amoje|amoji|amojo|amoms|amose|amųjų|ančia|ančią|ančio|ančiu|ančių|antis|antys|ąsias|ausia|ausią|ausio|ausiu|ausių|čiais|čiame|čiaus|čioje|čioms|čiose|damas|damos|davai|davau|davęs|davom|davos|davot|davus|ėliau|esnei|esnes|esnės|esnio|esnis|esniu|esnių|iajai|iajam|iamai|iamam|iamas|iamės|iamos|iamus|ianti|iantį|iasis|iatės|iausi|iedvi|ijose|imais|imąja|imąją|imąjį|imame|imams|imasi|imąsi|imesi|imoje|imoji|imojo|imoms|imose|imosi|imųjų|imusi|imųsi|inčia|inčią|inčio|inčiu|inčių|intis|intys|iojoj|iomis|iuoju|iuose|iuosi|iusia|iusią|iusio|iusis|iusiu|iusių|kimės|kitės|ojoje|omais|omąja|omąją|omąjį|omame|omasi|omoje|omoji|omojo|omoms|omose|omųjų|osiom|osios|siant|siąsi|siesi|simai|simam|simas|simės|simos|simus|sitės|siuos|šiant|šiąsi|šiesi|šimai|šimam|šimas|šimės|šimos|šimus|šitės|šiuos|tajai|tajam|tasis|tieji|tiems|tinai|tinam|tinas|tinoj|tinom|tinos|tinus|tomis|tumei|tumėm|tumės|tumėt|tuoju|tuose|usiai|usiam|usias|usiem|usįjį|usioj|usiom|usios|usius|viejų|viems|ymais|ymams|ymąsi|ymesi|ymosi|ymusi|ymųsi)$");

	private static Pattern p7 = Pattern.compile("(aisi|ajai|ajam|amai|amam|amas|amės|amos|amus|anti|antį|asai|asis|atės|ausi|čiai|čiam|čias|čiau|čios|čius|dama|dami|davę|davo|edvi|eisi|ėmės|ėmis|ęsis|esne|esnė|esnę|esni|esnį|ėtės|iais|iąja|iąją|iąjį|iama|iamą|iame|iami|iamo|iams|iamu|iamų|iant|iasi|iate|iaus|idvi|iedu|ieji|iejų|iems|iesi|imai|imam|imas|imės|imis|imos|imui|imus|inti|intį|ioje|ioji|iojo|ioms|iose|isai|itės|iuje|iųjų|iumi|iuos|iusi|iusį|kime|kite|myse|ojoj|omai|omam|omas|omės|omis|omos|omus|otės|siąs|sies|sima|simą|sime|simi|simo|simu|simų|site|šiąs|šies|šima|šimą|šime|šimi|šimo|šimu|šimų|šite|tais|tąja|tąją|tąjį|tame|tasi|tiem|ties|tina|tiną|tini|tino|tinu|tinų|toje|toji|tojo|toms|tose|tsai|tųjų|tume|tųsi|umis|uodu|uoju|uose|uosi|usia|usią|usio|usis|usiu|usių|viem|vimi|vyje|ymai|ymam|ymas|ymui|ymus|ysis)$");

	private static Pattern p8 = Pattern.compile("(ąįį|ais|ąja|ąją|ąjį|ama|amą|ame|ami|amo|ams|amu|amų|ant|asi|ate|aus|čia|čią|čio|čiu|čių|eis|ėje|ėme|ems|ėms|ėse|ėsi|ęsi|ėte|iai|iam|ias|iąs|iat|iau|iem|ies|įjį|ijų|ima|imą|ime|imi|imo|ims|imu|imų|int|ioj|iom|ion|ios|isi|ite|iui|iuj|ium|iuo|ius|kim|kis|kit|mis|nie|oje|oji|ojo|oma|omą|ome|omi|omo|oms|omu|omų|ose|osi|ote|sai|sią|sim|sis|sit|siu|šią|šim|šis|šit|šiu|tai|tam|tas|tim|tis|tos|tum|tus|tųs|tys|udu|uje|ųjį|ųjų|umi|ums|uos|usi|usį|vęs|vim|vyj|yje|ymą|yme|ymo|ymu|ymų|yse)$");

	private static Pattern p9 = Pattern.compile("(ai|am|an|as|ąs|at|au|ei|ėj|ėm|ėn|es|ės|ęs|ėt|ia|ią|ie|im|in|io|is|įs|it|iu|ių|ki|ms|oj|om|on|os|ot|si|sų|ši|ta|tą|ti|tį|to|ts|tu|tų|ui|uj|um|uo|us|ūs|ve|vi|vo|yj|ys)$");

	private static Pattern p10 = Pattern.compile("(a|ą|e|ė|ę|i|į|k|o|s|š|t|u|ų|y)$");

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
		
		//Mārcis: We inserted longer ending removal here:
		if (matcher.find()) return s;
		
		matcher = p2_5.matcher(s);
		if (matcher.find()){
			if (s.length()>=16)
				return s.substring(0,s.length()-14);
		}
		
		matcher = p2_4.matcher(s);
		if (matcher.find()){
			if (s.length()>=15)
				return s.substring(0,s.length()-13);
		}
		
		matcher = p2_3.matcher(s);
		if (matcher.find()){
			if (s.length()>=14)
				return s.substring(0,s.length()-12);
		}
		
		matcher = p2_2.matcher(s);
		if (matcher.find()){
			if (s.length()>=13)
				return s.substring(0,s.length()-11);
		}
		
		matcher = p2_1.matcher(s);
		if (matcher.find()){
			if (s.length()>=12)
				return s.substring(0,s.length()-10);
		}
		//Mārcis: END OF ADDITIONAL CODE.

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
