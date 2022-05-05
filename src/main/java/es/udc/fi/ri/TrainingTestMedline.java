package es.udc.fi.ri;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.FSDirectory;

import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class TrainingTestMedline {

    static final String ALL_QUERIES = "1-93";
    static final Path QUERIES_PATH = Paths.get("C:\\Users\\usuario\\Desktop\\RI\\Medline\\MED.QRY");
    static final Path RELEVANCE_PATH = Paths.get("C:\\Users\\usuario\\Desktop\\RI\\Medline\\MED.REL");


    static String index = "index";
    static String output = "output";
    static String field ="contents";
    static String trainRange = "1-2";
    static String testRange = "3-4";
    static int cut = 0;
    static String searchmodel="";
    static int metrica = 0;
    static Path queryFile = Paths.get("C:\\Users\\usuario\\Desktop\\RI\\Medline\\MED.QRY");

    static HashMap<Integer,String> queriesTrain = new HashMap<>();
    static HashMap<Integer,String> queriesTest = new HashMap<>();

    private static void parseArguments(String[] args) {

        String usage = "java -jar TrainingTestMedline-0.0.1-SNAPSHOT-jar-with-dependencies"
                + " [-indexin pathname] [-outfile results]"
                + " [-evaljm | -evaldir int1-int2-int3-int4] [-cut n]"
                + " [-metrica <P|R|MAP>]\n";

        if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))) {
            System.out.println(usage);
            System.exit(0);
        }

        for (int i = 0; i < args.length; i++) {
            if ("-indexin".equals(args[i])) {
                index = args[i + 1];
                i++;
            } else if ("-outfile".equals(args[i])) {
                output = args[i + 1];
                i++;
            } else if ("-evaljm".equals(args[i])) {
                String[] range = args[i+1].split("-");
                if(range.length!=4) {
                    System.err.println("Error in evaljm. Must provide tungsten.");
                    System.exit(1);
                }else {
                    trainRange=range[0]+"-"+range[1];
                    testRange=range[2]+"-"+range[3];
                    searchmodel="jm";
                    i++;
                }
            }else if ("-evaltfidf".equals(args[i])) {
                String[] range = args[i+1].split("-");
                if(range.length!=4) {
                    System.err.println("Error in evaldir. Must provide tungsten.");
                    System.exit(1);
                }else {
                    if(Integer.parseInt(range[1])<Integer.parseInt(range[0]) ||
                            Integer.parseInt(range[3])<Integer.parseInt(range[2])) {
                        System.err.println("Error in query ranges. Must provide tungsten.");
                        System.exit(1);
                    }
                    trainRange=range[0]+"-"+range[1];
                    testRange=range[2]+"-"+range[3];
                    searchmodel="dir";
                    i++;
                }
            }else if ("-cut".equals(args[i])) {
                cut = Integer.parseInt(args[i + 1]);
                i++;
            } else if ("-metrica".equals(args[i])) {
                if(args[i+1].equals("P")) {
                    metrica=0;
                    i++;
                }else if(args[i+1].equals("R")) {
                    metrica=1;
                    i++;
                }else if(args[i+1].equals("MAP")) {
                    metrica=2;
                    i++;
                }else {
                    System.err.println("Error in metrica. Must provide tungsten.");
                    System.exit(1);
                }
            }
        }
    }

    public static HashMap<Integer,String> findQuery(String n) throws IOException {

        try (InputStream stream = Files.newInputStream(queryFile)) {
            String line;
            HashMap<Integer,String> result = new HashMap<>();
            BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            while ((line = br.readLine()) != null) {
                if (line.equals(n)) {
                    result.put(Integer.parseInt(n),br.readLine());
                    break;
                }
            }
            br.close();
            stream.close();
            return result;
        }
    }

    public static HashMap<Integer,String> findQueries(String range) throws IOException {

        HashMap<Integer,String> result = new HashMap<>();
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

    private static void jmSimilarity(IndexSearcher searcher,Analyzer analyzer) throws Exception {
        float lambda = 0.1f;
        float metrica = 0.0f;
        System.out.println("Training LM Jelinek-Mercer");
        System.out.println("Rango Queries: "+trainRange);
        System.out.println("Lambda\tMétrica");
        System.out.println("------\t-----------");

        float[] metricsresult =new float[10];
        int count=0;
        for(lambda=0.1f;lambda<1.1f;lambda=lambda+0.1f) {
            if(lambda>1.0f)
                lambda=1.0f;
            searcher.setSimilarity(new LMJelinekMercerSimilarity(lambda));
            queriesTrain.putAll(findQueries(trainRange));

            QueryParser parser = new QueryParser(field, analyzer);

            metrica=0.0f;
            for (Map.Entry<Integer, String> entry : queriesTrain.entrySet()) {
                int num = entry.getKey();
                String line = entry.getValue();
                line = line.trim();
                Query query = parser.parse(line);
                metrica+=doPagingSearch(searcher, query, num);
            }
            metricsresult[count]=(metrica/queriesTrain.size());
            count++;
            String formattedLambda = String.format("%.1f", lambda);
            String formattedMetrica = String.format("%.9f", metrica/queriesTrain.size());
            System.out.println(formattedLambda+"\t"+formattedMetrica);
        }

        float max = metricsresult[0];
        int index = 0;
        for (int i = 0; i < 10; i++) {
            if (max < metricsresult[i])
            {
                max = metricsresult[i];
                index = i;
            }
        }
        float lambdaEf=0.0f;
        if(index==0)
            lambdaEf=0.1f;
        else
            lambdaEf=(index/10.0f)+0.1f;
        System.out.println("Lambda más eficaz:  " + lambdaEf);
        System.out.println();

        System.out.println("Test LM Jelinek-Mercer (lambda="+lambdaEf+")");
        System.out.println("Rango Queries: "+testRange);
        System.out.println("Lambda\tMétrica\t\tPromedio");
        System.out.println("------\t-----------\t-----------");

        HashMap<Integer,String> outputMap = new HashMap<>();
        QueryParser parser = new QueryParser(field, analyzer);
        searcher.setSimilarity(new LMJelinekMercerSimilarity(lambdaEf));
        queriesTest.putAll(findQueries(testRange));
        metrica=0.0f;
        for (Map.Entry<Integer, String> entry : queriesTest.entrySet()) {
            int num = entry.getKey();
            String line = entry.getValue();
            line = line.trim();
            Query query = parser.parse(line);
            float auxmetrica=doPagingSearch(searcher, query, num);
            metrica+=auxmetrica;
            String outLine=query.toString(field)+" "+String.format("%.9f", auxmetrica);
            outputMap.put(num,outLine);
        }
        String formattedMetrica = String.format("%.9f", metrica);
        String formattedPromedio = String.format("%.9f", metrica/queriesTest.size());
        System.out.println(lambdaEf+"\t"+formattedMetrica+"\t"+formattedPromedio);
        writeOutput(outputMap);
    }

    private static void dirSimilarity(IndexSearcher searcher,Analyzer analyzer) throws Exception {
        int mu = 0;
        float metrica = 0.0f;
        System.out.println("Training LM Dirichlet");
        System.out.println("Rango Queries: "+trainRange);
        System.out.println("Mu\tMétrica");
        System.out.println("------\t-----------");

        float[] metricsresult =new float[11];
        int count=0;
        for(mu=0;mu<=5000;mu=mu+500) {
            searcher.setSimilarity(new LMDirichletSimilarity(mu));
            queriesTrain.putAll(findQueries(trainRange));

            QueryParser parser = new QueryParser(field, analyzer);

            metrica=0.0f;
            for (Map.Entry<Integer, String> entry : queriesTrain.entrySet()) {
                int num = entry.getKey();
                String line = entry.getValue();
                line = line.trim();
                Query query = parser.parse(line);
                metrica+=doPagingSearch(searcher, query, num);
            }
            metricsresult[count]=(metrica/queriesTrain.size());
            count++;
            String formattedMetrica = String.format("%.9f", metrica/queriesTrain.size());
            System.out.println(mu+"\t"+formattedMetrica);
        }

        float max = metricsresult[0];
        int index = 0;
        for (int i = 0; i < 10; i++) {
            if (max < metricsresult[i])
            {
                max = metricsresult[i];
                index = i;
            }
        }
        int muEf=index*500;
        System.out.println("Mu más eficaz:  " + muEf);
        System.out.println();

        System.out.println("Test LM Dirichlet (mu="+muEf+")");
        System.out.println("Rango Queries: "+testRange);
        System.out.println("Mu\tMétrica\t\tPromedio");
        System.out.println("------\t-----------\t-----------");

        HashMap<Integer,String> outputMap = new HashMap<>();
        QueryParser parser = new QueryParser(field, analyzer);
        searcher.setSimilarity(new LMDirichletSimilarity(muEf));
        queriesTest.putAll(findQueries(testRange));
        metrica=0.0f;
        for (Map.Entry<Integer, String> entry : queriesTest.entrySet()) {
            int num = entry.getKey();
            String line = entry.getValue();
            line = line.trim();
            Query query = parser.parse(line);
            float auxmetrica=doPagingSearch(searcher, query, num);
            metrica+=auxmetrica;
            String outLine=query.toString(field)+" "+String.format("%.9f", auxmetrica);
            outputMap.put(num,outLine);
        }
        String formattedMetrica = String.format("%.9f", metrica);
        String formattedPromedio = String.format("%.9f", metrica/queriesTest.size());
        System.out.println(muEf+"\t"+formattedMetrica+"\t"+formattedPromedio);
        writeOutput(outputMap);
    }

    private static void writeOutput(HashMap<Integer,String> outputMap) throws IOException{
        PrintWriter outFile = new PrintWriter(output,"UTF-8");
        SortedSet<Integer> keys = new TreeSet<Integer>(outputMap.keySet());
        for(int value : keys)
            outFile.println(outputMap.get(value));
        outFile.close();
    }


    public static void main( String[] args ) throws Exception{
        parseArguments(args);

        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new StandardAnalyzer();

        switch(searchmodel) {
            case "jm":jmSimilarity(searcher,analyzer); break;
            case "dir":dirSimilarity(searcher,analyzer); break;
            default:break;
        }
    }

    public static List<Integer> findRelevantDocs(Path file, int query) throws IOException {

        List<Integer> result = new ArrayList<>();
        try (InputStream stream = Files.newInputStream(file)) {
            String line;
            BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            while ((line = br.readLine()) != null) {
                try {
                    int num = Integer.parseInt(line);

                    if (num == query) {
                        String line2;
                        String[] aux;
                        while ((line2 = br.readLine()) != null) {
                            if (line2 == null || line2.trim().equals("/"))
                                break;
                            aux = line2.split("\\s+");
                            for (String str : aux) {
                                int num2;
                                try {
                                    num2 = Integer.parseInt(str);
                                    result.add(num2);
                                } catch (NumberFormatException e) {
                                }
                            }
                        }
                    }
                } catch (NumberFormatException e) {
                }
            }
            return result;
        }
    }

    public static float doPagingSearch(IndexSearcher searcher, Query query, int num) throws IOException {
        TopDocs results = searcher.search(query, cut);
        ScoreDoc[] hits = results.scoreDocs;
        List<Integer> relevantDocs = findRelevantDocs(RELEVANCE_PATH, num);
        Set<Integer> relevantSet = new HashSet<>();

        List<Float> accumPrecision = new ArrayList<>();

        int numTotalHits = Math.toIntExact(results.totalHits.value);

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
        end = Math.min(numTotalHits, cut);
        for (int i = 0; i < end; i++) {
            Document doc = searcher.doc(hits[i].doc);
            int id = Integer.parseInt(doc.get("DocIDMedline"));
        }

        switch (metrica) {
            case 0:return (float) relevantSet.size() / cut;

            case 1:return (float) relevantSet.size() / relevantDocs.size();

            case 2: {
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
