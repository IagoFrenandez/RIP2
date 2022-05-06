package es.udc.fi.ri;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.LambdaDF;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class TrainingTestMedline {

    static HashMap<Integer, String> queries = new HashMap<>();
    static HashMap<Integer, String> queries2 = new HashMap<>();
    static final String ALL_QUERIES = "1-30";
    static final Path QUERIES_PATH = Paths.get("C:\\Users\\iagof\\Desktop\\RI\\med.tar\\MED.QRY");
    static final Path RELEVANCE_PATH = Paths.get("C:\\Users\\iagof\\Desktop\\RI\\med.tar\\MED.REL");
    static Path queryFile = QUERIES_PATH;
    static int queryCount=0;
    static int queryMode =0;
    static String queryRange = "1-3";
    static String queryRange2 = "6-8";
    static String queryNum = "3";
    static String field = "contents";
    static String index = "index";


    public static HashMap<Integer, String> findQuery(String n) throws IOException {
        try (InputStream stream = Files.newInputStream(queryFile)) {
            String line;
            String aux = "";
            int m= Integer.parseInt(n)+1;
            HashMap<Integer, String> result = new HashMap<>();
            BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            while ((line = br.readLine()) != null) {
                if (line.equals(".I " + n)  ) {
                    br.readLine();
                    aux= aux + br.readLine();
                    line= br.readLine();
                    while (!line.equals(".I " + String.valueOf(m)) ) {
                        aux= aux + line;
                        line= br.readLine();
                        if (line == null){
                            result.put(Integer.parseInt(n), aux.toLowerCase(Locale.ROOT));
                            break;}
                    }
                    result.put(Integer.parseInt(n), aux.toLowerCase(Locale.ROOT));
                    break;
                }
            }
            br.close();
            stream.close();
            return result;
        }
    }
    public static HashMap<Integer, String> findQueries(String range) throws IOException {
        HashMap<Integer, String> result = new HashMap<>();
        String nums[] = range.split("-");

        if (nums.length != 2) {
            System.err.println("Query range is in an incorrect format; it must be Int1-Int2");
            System.exit(1);
        }
        int top = Integer.parseInt(nums[0]);
        int bot = Integer.parseInt(nums[1]);

        for (int i = top; i <= bot; i++) {
            result.putAll(findQuery(String.valueOf(i)));
        }
        return result;
    }
    public static void evaltfidf(IndexSearcher searcher, Analyzer analyzer,int cut, String metrica) throws IOException, ParseException {
        int num = 0;
        float aux = 0.0f;
        queries.putAll(findQueries(queryRange));
        QueryParser parser = new QueryParser(field, analyzer);
        searcher.setSimilarity(new ClassicSimilarity());
        for (Map.Entry<Integer, String> entry : queries.entrySet()) {
            num = entry.getKey();
            String line = entry.getValue();
            line = line.trim();
            Query query = parser.parse(QueryParser.escape(line));
            aux= doPagingSearch(searcher, query, num,cut,metrica);
            System.out.println("Para la query "+num+" su metrica en "+metrica+" es " +aux);}
    }
    public static void evaljm(IndexSearcher searcher, Analyzer analyzer,int cut, String metrica) throws IOException, ParseException {
        float lambda=0.1f;
        float auxlambda=0.0f;
        float metricas = 0.0f;
        float aux = 0.0f;
        float aux2[] = new float[11];
        int num = 0;
        System.out.println("Mensaje de entrenamiento");
         queries.putAll(findQueries(queryRange));
        queries2.putAll(findQueries(queryRange2));
        QueryParser parser = new QueryParser(field, analyzer);
        FileWriter documento= new FileWriter(index+"/"+"medline.jm.training."+queryRange+".test."+queryRange2+"."+metrica+cut+".training.csv",true);
        documento.append(metrica+";"+cut+"\n");
       documento.append("query;0.1;0.2;0.3;0.4;0.5;0.6;0.7;0.8;0.9;1.0\n");
        for (Map.Entry<Integer, String> entry : queries.entrySet()) {
            documento.append(entry.getKey()+";");
            for ( lambda=1; lambda<11; lambda=lambda+1) {
            searcher.setSimilarity(new LMJelinekMercerSimilarity(lambda/10));
                    num = entry.getKey();
                    String line = entry.getValue();
                    line = line.trim();
                    Query query = parser.parse(QueryParser.escape(line));
                    aux= doPagingSearch(searcher, query, num,cut,metrica);
                System.out.println("Para la query "+num+" su metrica en "+metrica+" es " +aux);
                    documento.write(String.valueOf(aux)+";");
                    aux2[(int) lambda]+=aux;
                if (aux>metricas){
                    metricas=aux;
                    auxlambda=lambda/10;
                }
            }
            documento.append("\n");
        }
        documento.append("Promedio:;");
        for (int i=1;i<aux2.length;i++){
            documento.append((aux2[i]/aux2.length)+";");}
        documento.close();
        FileWriter documento2= new FileWriter(index+"/"+"medline.jm.training."+queryRange+".test."+queryRange2+"."+metrica+cut+".test.csv",true);
        searcher.setSimilarity(new LMJelinekMercerSimilarity(auxlambda));
        documento.append(auxlambda+";"+metrica+"\n");
        for (Map.Entry<Integer, String> entry : queries2.entrySet()) {
            num = entry.getKey();
            String line = entry.getValue();
            line = line.trim();
            Query query = parser.parse(QueryParser.escape(line));
            aux= doPagingSearch(searcher, query, num,cut,metrica);
            System.out.println("Para la query "+num+" su metrica en "+metrica+" es " +aux);}
        System.out.println("Mejor metrica "+ metricas);
        }


    public static void main(String[] args) throws Exception {
        String usage = "Usage:\tjava org.apache.lucene.demo.SearchFiles [-index dir] [-field f] [-repeat n] [-queries file] [-query string] [-raw] [-paging hitsPerPage] [-knn_vector knnHits]\n\nSee http://lucene.apache.org/core/9_0_0/demo/ for details.";
        if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))) {
            System.out.println(usage);
            System.exit(0);
        }
        String indexingmodel = "tfidf";

        String field = "contents";
        String metrica = "metrica";
        List<Float> metrics = new ArrayList<>();
        float lambda = 0.5f;
        int cut = 1;
        int top = 1;
        boolean raw = false;
        int knnVectors = 0;
        String queryString = null;
        int hitsPerPage = 10;
        boolean tfidf = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-indexin":
                    index = args[++i];
                    System.out.println("Index: " + index);
                    break;
                case "-evaltfidf":
                    tfidf = true;
                    queryRange= args[i+1];
                    ++i;
                    break;
                case "-evaljm":
                    tfidf = false;
                    System.out.println(args[i]);
                    queryRange= args[i+1];
                    queryRange2=args[i+2];
                   ;i++;
                   i++;
                    break;
                case "-cut":
                    cut = Integer.parseInt(args[++i]);
                    if (cut <= 0) {
                        System.err.println("There must be at least 1 hit per page.");
                        System.exit(1);
                    }
                    break;
                case "-metrica":
                    metrica = args[++i];
                    break;

                default:
                    System.err.println("Unknown argument: " + args[i]);
                    System.exit(1);
            }
        }

        DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new StandardAnalyzer();
        if (tfidf) {
            evaltfidf(searcher, analyzer,cut,metrica);
        }else evaljm(searcher, analyzer,cut,metrica);




        reader.close();
    }


    /**
     * This demonstrates a typical paging search scenario, where the search engine
     * presents pages of size n to the user. The user can then go to the next page
     * if interested in the next hits.
     *
     * <p>
     * When the query is executed for the first time, then only enough results are
     * collected to fill 5 result pages. If the user wants to page beyond this
     * limit, then the query is executed another time and all hits are collected.
     */
    public static List<Integer> findRelevantDocs(Path file, int query) throws IOException {
        List<Integer> relevantDocs = new ArrayList<>();
        BufferedReader reader = Files.newBufferedReader(file);
        String line = reader.readLine();
        while (line != null) {
            String[] tokens = line.split(" ");
            if (Integer.parseInt(tokens[0])==query) {
                relevantDocs.add(Integer.parseInt(tokens[2]));

            }
            line = reader.readLine();

        }
        reader.close();
        return relevantDocs;

    }

    public static float doPagingSearch(IndexSearcher searcher, Query query, int num, int cut, String metrica) throws IOException {
        TopDocs results = searcher.search(query, cut);
        ScoreDoc[] hits = results.scoreDocs;
        List<Integer> relevantDocs = findRelevantDocs(RELEVANCE_PATH, num);
        Set<Integer> relevantSet = new HashSet<>();

        List<Float> accumPrecision = new ArrayList<>();

      //  System.out.println("RELEVANT DOCS = " + relevantDocs.toString());
        int numTotalHits = Math.toIntExact(results.totalHits.value);
      //  System.out.println(numTotalHits + " total matching documents");

        //this loop is used for calculating the metrics
        int end = Math.min(numTotalHits, cut);
        int n = 1;
        for (int i = 0; i < end; i++) {
            Document doc = searcher.doc(hits[i].doc);
            int id = Integer.parseInt(doc.get("DocIDMedline"));
            for (int idaux : relevantDocs) {
                if (id == idaux) {
                    relevantSet.add(id);
                    float prec = (float) relevantSet.size() / n;
                    accumPrecision.add(prec);
                }
            }
            n++;
        }

        //this is the printing loop, it displays the top TOP hit documents
        switch (metrica) {
            //P
            case ("P"):
                return (float) relevantSet.size() / cut;
            //R
            case ("R"):
                return (float) relevantSet.size() / relevantDocs.size();
            //MAP
            case ("MAP"): {
                if (relevantSet.size() != 0) {
                    float sum = 0;
                    for (Float d : accumPrecision)
                        sum += d;
                    return (float) sum / relevantSet.size();
                }
            }
        }
        return 0.0f;
    }

}
