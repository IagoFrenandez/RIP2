
package es.udc.fi.ri;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class IndexMedline {


    static String indexPath = "index"; //default index path is a folder named index located in the root dir
    static Path docPath;
    static String indexingmodel = "tfidf";
    static float lambda = 0.5f;
    static float mu = 0.5f;
    static IndexWriterConfig.OpenMode openmode = IndexWriterConfig.OpenMode.CREATE;

    private IndexMedline() {
    }


    private static void readConfigFile(String path) {

        FileInputStream inputStream;
        Properties prop = new Properties();

        try {
            inputStream = new FileInputStream(path);
            prop.load(inputStream);
        } catch (IOException ex) {
            System.out.println("Error reading config file: " + ex);
            System.exit(-1);
        }

        //Read and store doc path
        String docsList = prop.getProperty("docs");
        if (docsList != null) {
            String[] docsSplit = docsList.split(" ");
            docPath = Paths.get(docsSplit[0]);

        } else {
            System.out.println("Error in the config file, there is no doc path");
            System.exit(-1);
        }

        //Read and store doc path
        String im = prop.getProperty("indexingmodel");
        if (im != null) {
            String[] imsplit = im.split(" ");
            if (imsplit.length == 2) {
                indexingmodel = imsplit[0];
                if (imsplit[0].equals("jm"))
                    lambda = Float.parseFloat(imsplit[1]);
                else if (imsplit[0].equals("dir"))
                    mu = Float.parseFloat(imsplit[1]);
                else
                    System.out.println("Error reading Indexing Model, defaulting to tfidf");
            } else if (imsplit.length == 1 && imsplit[0].equals("tfidf"))
                indexingmodel = imsplit[0];
            else
                System.out.println("Error reading Indexing Model, defaulting to tfidf");
        } else {
            System.out.println("Error reading Indexing Model, defaulting to tfidf");
        }
    }

    public static final FieldType TYPE_STORED = new FieldType();

    static final IndexOptions options = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;

    static {
        TYPE_STORED.setIndexOptions(options);
        TYPE_STORED.setTokenized(true);
        TYPE_STORED.setStored(true);
        TYPE_STORED.setStoreTermVectors(true);
        TYPE_STORED.setStoreTermVectorPositions(true);
        TYPE_STORED.freeze();
    }

    static void indexDoc(IndexWriter writer, Path file) throws IOException {

        try (InputStream stream = Files.newInputStream(file)) {
            String line;
            BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            while ((line = br.readLine()) != null) {
                int num;
                try {
                    num = Integer.parseInt(line);
                    String contents = "";
                    Document doc = new Document();

                    String line2;
                    while ((line2 = br.readLine()) != null) {
                        if (line2 == null || line2.trim().equals("/"))
                            break;
                        contents += line2 + " ";
                    }
                    doc.add(new Field("DocIDMedline", String.valueOf(num), TYPE_STORED));

                    doc.add(new Field("contents", contents, TYPE_STORED));

                    if (writer.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE) {
                        writer.addDocument(doc);
                    } else {
                        writer.updateDocument(new Term("path", file.toString()), doc);
                    }
                } catch (NumberFormatException e) {
                }
            }
        }

    }

    public static void main(String[] args) {

        String indexPath = "index";
        String docPath = null;

        String vectorDictSource = null;
        Path docDir= null;
        String create = "create";

        int para=0;
        String usage = "java -jar IndexMedline-0.0.1-SNAPSHOT-jar-with-dependencies"
                + " [-index INDEX_PATH] [-openmode <APPEND | CREATE | APPEND_OR_CREATE>]\n";
        for (int i = 0; i < args.length; i++) {

            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docs":
                    docPath = args[++i];
                    break;
                case "-create":
                    create = "create";
                    break;
                case "-openmode":
                    create = args[++i];
                    break;
                case "-indexingmodel":
                    indexingmodel = args[++i];
                    if (indexingmodel.equals("jm"))
                        lambda = Float.valueOf(args[++i]);
                    break;
                default:

                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }


        Date start = new Date();
        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            Directory dir = FSDirectory.open(Paths.get(indexPath));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            iwc.setOpenMode(openmode);

            switch (indexingmodel) {
                case "jm":
                    iwc.setSimilarity(new LMJelinekMercerSimilarity(lambda));
                    break;
                case "tfidf":
                    iwc.setSimilarity(new ClassicSimilarity());
                    break;
                default:
                    iwc.setSimilarity(new ClassicSimilarity());
                    break;
            }

            IndexWriter writer = new IndexWriter(dir, iwc);

            indexDoc(writer, Path.of(docPath));

            try {
                writer.commit();
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            Date end = new Date();
            System.out.println(end.getTime() - start.getTime() + " total milliseconds");

        } catch (IOException e) {
            System.out.println("Caught a " + e.getClass() + " with message: " + e.getMessage());
        }
    }
}
