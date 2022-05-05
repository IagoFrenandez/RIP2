
package es.udc.fi.ri;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.compound.hyphenation.PatternParser;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.demo.knn.KnnVectorDict;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.PublicKey;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class IndexMedline {
    //Medline

    static final String KNN_DICT = "knn-dict";
    static String indexPath = "index"; //default index path is a folder named index located in the root dir
    static Path docPath;
    static String indexingmodel = "tfidf";
    static float lambda = 0.5f;
    static float mu = 0.5f;
    static IndexWriterConfig.OpenMode openmode = IndexWriterConfig.OpenMode.CREATE;

    private IndexMedline() {
    }
static void parserall(Path file, Path indexpath) throws IOException {
    try (InputStream stream = Files.newInputStream(file)) {
        String line;
        String aux = "";
        String[] aux2;
        int n=1;
        File auxfile = new File(indexpath.toString()+"/"+"meddocs");
        auxfile.mkdirs();
        BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        line = br.readLine();
        while (line  != null) {
            if (line.equals(".I " + n)  ) {
                FileWriter documento= new FileWriter(indexpath.toString()+"/"+"meddocs/"+n+".txt",true);
                PrintWriter pw = new PrintWriter(documento);
                line=br.readLine();
                System.out.println(line);
                if ((line).equals(".W")){
                    line=br.readLine();
                    pw.println(n);
                    while (!line.equals(".I " + String.valueOf(n+1)) ) {
                        pw.println(line);
                        line= br.readLine();
                        if (line == null){
                            documento.close();
                            break;}
                }
                }
                documento.close();
            }

            n++;
        }
    } catch (IOException e) {
        e.printStackTrace();
    }
}

    static void indexDocs(final IndexWriter writer, Path path) throws IOException, AccessDeniedException {

        if (Files.isDirectory(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    try {
                        indexDoc(writer, file);        //toMillis para saber cuanto tarda en indexar en milisegundos
                    } catch (@SuppressWarnings("unused")
                            IOException ignore) {
                        ignore.printStackTrace(System.err);
                        // don't index files that can't be read.
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } else {
            indexDoc(writer, path);
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
                    if (writer.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE) {
                        // New index, so we just add the document (no old document can be there):
                        System.out.println("adding " + file);
                        writer.addDocument(doc);
                    } else {
                        // Existing index (an old copy of this document may have been indexed) so
                        // we use updateDocument instead to replace the old one matching the exact
                        // path, if present:
                        System.out.println("updating " + file);
                        writer.updateDocument(new Term("path", file.toString()), doc);
                    }
                } catch (NumberFormatException e) {
                }
            }
        }

    }

    public static void main(String[] args) throws IOException {

        String indexPath = "index";
        Path docPath = null;

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
                    docPath = Path.of(args[++i]);
                    break;
                case "-knn_dict":
                    vectorDictSource = args[++i];
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
        parserall(Path.of("C:\\Users\\iagof\\Desktop\\RI\\med.tar\\MED.ALL"),Path.of("C:\\Users\\iagof\\Desktop\\RI\\Pruebas"));
        if (!Files.isReadable(docPath)) {
            System.out.println("Document directory '" + docPath.toAbsolutePath()
                    + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }
        Date start = new Date();
        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            Directory dir = FSDirectory.open(Paths.get(indexPath));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            KnnVectorDict vectorDictInstance = null;
            long vectorDictSize = 0;
            if (vectorDictSource != null) {
                KnnVectorDict.build(Paths.get(vectorDictSource), dir, KNN_DICT);
                vectorDictInstance = new KnnVectorDict(dir, KNN_DICT);
                vectorDictSize = vectorDictInstance.ramBytesUsed();
            }

            switch (create) {
                case "create":
                    // Create a new index in the directory, removing any
                    // previously indexed documents:
                    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
                    break;
                case "create_or_append":
                    // Create a new index in the directory, removing any
                    // previously indexed documents:
                    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
                    break;
                case "append":
                    // Create a new index in the directory, removing any
                    // previously indexed documents
                    iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
                    break;
                default:
                    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            }
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

            try(IndexWriter writer = new IndexWriter(dir, iwc)){

            indexDocs(writer, docPath);
            } finally {
                IOUtils.close(vectorDictInstance);
            }
            Date end = new Date();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                System.out.println(
                        "Indexed "
                                + reader.numDocs()
                                + " documents in "
                                + (end.getTime() - start.getTime())
                                + " milliseconds");
                if (reader.numDocs() > 100
                        && vectorDictSize < 1_000_000
                        && System.getProperty("smoketester") == null) {
                    throw new RuntimeException(
                            "Are you (ab)using the toy vector dictionary? See the package javadocs to understand why you got this exception.");
                }
            }
        } catch (IOException e) {
            System.out.println("Caught a " + e.getClass() + " with message: " + e.getMessage());
        }
    }
}
