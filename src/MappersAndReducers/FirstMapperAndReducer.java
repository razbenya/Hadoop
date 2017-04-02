package MappersAndReducers;

import Model.Amount;
import Model.PairInDecade;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

/**
 * Created by snoop_000 on 01/03/2017.
 */
public class FirstMapperAndReducer {

    public static boolean filter(ArrayList<String> list,String word1,String word2){
        for(String ele : list){
            if(ele.toLowerCase().equals(word1.toLowerCase()) || ele.toLowerCase().equals(word2.toLowerCase()))
                return true;
        }
        return false;
    }

    public static class MapperClass extends Mapper<LongWritable, Text, PairInDecade, Amount> {
        private static final String[] engStopwords = {"&","+","~",",","%","'","-","*",".","!",";",":","\"","(",")","a", "as", "able", "about", "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again", "against", "aint", "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "arent", "around", "as", "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "cmon", "cs", "came", "can", "cant", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldnt", "course", "currently", "definitely", "described", "despite", "did", "didnt", "different", "do", "does", "doesnt", "doing", "dont", "done", "down", "downwards", "during", "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "far", "few", "ff", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth", "four", "from", "further", "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got", "gotten", "greetings", "had", "hadnt", "happens", "hardly", "has", "hasnt", "have", "havent", "having", "he", "hes", "hello", "help", "hence", "her", "here", "heres", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", "however", "i", "id", "ill", "im", "ive", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isnt", "it", "itd", "itll", "its", "its", "itself", "just", "keep", "keeps", "kept", "know", "knows", "known", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "little", "look", "looking", "looks", "ltd", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not", "nothing", "novel", "now", "nowhere", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "particular", "particularly", "per", "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provides", "que", "quite", "qv", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively", "respectively", "right", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she", "should", "shouldnt", "since", "six", "so", "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup", "sure", "ts", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "thats", "thats", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "theres", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "theyd", "theyll", "theyre", "theyve", "think", "third", "this", "thorough", "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two", "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually", "value", "various", "very", "via", "viz", "vs", "want", "wants", "was", "wasnt", "way", "we", "wed", "well", "were", "weve", "welcome", "well", "went", "were", "werent", "what", "whats", "whatever", "when", "whence", "whenever", "where", "wheres", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whos", "whoever", "whole", "whom", "whose", "why", "will", "willing", "wish", "with", "within", "without", "wont", "wonder", "would", "would", "wouldnt", "yes", "yet", "you", "youd", "youll", "youre", "youve", "your", "yours", "yourself", "yourselves", "zero"};
        private static final String[] hebStopWords = {"אני","&","+","~",",","%","'","-","*",".","!",";",":","\"","(",")", "את", "אתה", "אנחנו", "אתן", "אתם", "הם", "הן", "היא", "הוא", "שלי", "שלו", "שלך", "שלה", "שלנו", "שלכם", "שלכן", "שלהם", "שלהן", "לי", "לו", "לה", "לנו", "לכם", "לכן", "להם", "להן", "אותה", "אותו", "זה", "זאת", "אלה", "אלו", "תחת", "מתחת", "מעל", "בין", "עם", "עד", "נגר", "על", "אל", "מול", "של", "אצל", "כמו", "אחר", "אותו", "בלי", "לפני", "אחרי", "מאחורי", "עלי", "עליו", "עליה", "עליך", "עלינו", "עליכם", "לעיכן", "עליהם", "עליהן", "כל", "כולם", "כולן", "כך", "ככה", "כזה", "זה", "זות", "אותי", "אותה", "אותם", "אותך", "אותו", "אותן", "אותנו", "ואת", "את", "אתכם", "אתכן", "איתי", "איתו", "איתך", "איתה", "איתם", "איתן", "איתנו", "איתכם", "איתכן", "יהיה", "תהיה", "היתי", "היתה", "היה", "להיות", "עצמי", "עצמו", "עצמה", "עצמם", "עצמן", "עצמנו", "עצמהם", "עצמהן", "מי", "מה", "איפה", "היכן", "אם", "לאן",  "איזה", "מהיכן", "איך", "כיצד",  "מתי", "כאשר", "כש", "למרות", "לפני", "אחרי",  "למה", "מדוע", "כי", "יש", "אין", "אך", "מנין", "מאין", "מאיפה", "יכל", "יכלה", "יכלו", "יכול", "יכולה", "יכולים", "יכולות", "יוכלו", "יוכל", "מסוגל", "לא", "רק", "אולי", "אין", "לאו", "אי", "כלל", "נגד", "אם", "עם", "אל", "אלה", "אלו", "אף", "על", "מעל", "מתחת", "מצד", "בשביל", "לבין", "באמצע", "בתוך", "דרך", "מבעד", "באמצעות", "למעלה", "למטה", "מחוץ", "מן", "לעבר", "מכאן", "כאן", "הנה", "הרי", "פה", "שם", "אך", "ברם", "שוב", "אבל", "מבלי", "בלי", "מלבד", "רק", "בגלל", "מכיוון", "עד", "אשר", "ואילו", "למרות", "אס", "כמו", "כפי", "אז", "אחרי", "כן", "לכן", "לפיכך", "מאד", "עז", "מעט", "מעטים", "במידה", "שוב", "יותר", "מדי", "גם", "כן", "נו", "אחר", "אחרת", "אחרים", "אחרות", "אשר", "או"};
        private static ArrayList<String> wordsList;
        private static boolean includeStopWords;
        private boolean filterStopWords;

        protected void setup(Context context) throws IOException, InterruptedException {
            if(context.getConfiguration().get("language","eng").equals("eng"))
                wordsList = new ArrayList<>((Arrays.asList(engStopwords)));
            else
                wordsList = new ArrayList<>((Arrays.asList(hebStopWords)));
            includeStopWords = context.getConfiguration().get("stopWords","1").equals("1");
        }


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String[] line = (value.toString()).split("\t");
            String[] words = line[0].split(" ");
            if(words.length>1) {
                String w1 = words[words.length - 2];
                String w2 = words[words.length - 1];
                if (!includeStopWords)
                    filterStopWords = filter(wordsList, w1, w2);
                if (includeStopWords || !filterStopWords) {
                    context.write(
                            new PairInDecade(w1 , w2 , (Integer.parseInt(line[1]) / 10)),
                            new Amount(Integer.parseInt(line[2])));
                }
            }
        }
    }

    public static class Reduce extends Reducer<PairInDecade, Amount, PairInDecade, Amount> {
        public void reduce(PairInDecade pairInDecade, Iterable<Amount> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Amount amount : values) {
                sum += amount.getAmount();
            }
            context.write(new PairInDecade(pairInDecade.getWord1(),pairInDecade.getWord2(),pairInDecade.getDecade()), new Amount(sum));
        }
    }

}
