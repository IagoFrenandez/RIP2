package es.udc.fi.ri;/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.demo.knn.DemoEmbeddings;
import org.apache.lucene.demo.knn.KnnVectorDict;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

/**
 * Index all text files under a directory.
 *
 * <p>This is a command-line application demonstrating simple Lucene indexing. Run it with no
 * command-line arguments for usage information.
 */



public class IndexFiles implements AutoCloseable {
    static final String KNN_DICT = "knn-dict";
    static boolean partialIndex = false;
    // Calculates embedding vectors for KnnVector search
    private static DemoEmbeddings demoEmbeddings = null;
    private final KnnVectorDict vectorDict;
    static int numThreads =Runtime.getRuntime().availableProcessors();
    static List<Path> docsPaths = new ArrayList<>();
    static int deep =999;
        //flag para saber si es partialindex o no Podemos moverlo al main
    static List<Path> partialIndexes = new ArrayList<>();   //Lista de Indices Parciales

    static boolean onlyFiles = true;
    static List<String> fileTypes = new ArrayList<>();
    static List<Path> docsPath = new ArrayList<>();
    static int topLines = 0;
    static int bottomLines = 0;
    static final String DEFAULT_PATH = "config.properties";



    public static class WorkerThread implements Runnable {

        private final List<Path> folders;
        private IndexWriter writer;
        private List<FSDirectory> partialDirs = new ArrayList<>();

        //Para IndexNonPartial
        public WorkerThread(final List<Path> folders, IndexWriter writer) {
            this.folders = folders;
            this.writer = writer;
        }

        //Para IndexPartial
        public WorkerThread(final List<Path> folders, List<FSDirectory> partialDirs) {
            this.folders = folders;
            this.partialDirs = partialDirs;
        }

        /**
         * This is the work that the current thread will do when processed by the pool.
         * In this case, it will only print some information.
         */
        @Override
        public void run() {

            String ThreadName = Thread.currentThread().getName();
            //IndexNoPartial
            if(!partialIndex) {
                for (Path path : folders) {
                        System.out.println(String.format("I am the thread '%s' and I am responsible for folder '%s'",
                        Thread.currentThread().getName(), path));
                    try {
                        System.out.println(ThreadName + ": Indexing to directory  " + path + "...");
                        indexDocs(writer, path);
                    } catch (IOException e) {
                        System.out.println(ThreadName + ": caught a " + e.getClass() + "whith message" + e.getMessage());
                    }
                }
            //IndexPartial
            }else{
                try{
                    Analyzer analyzer = new StandardAnalyzer();
                    IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

                    for (int i = 0; i < folders.size(); i++) {

                        IndexWriter writer = new IndexWriter(partialDirs.get(i), iwc);

                        System.out.println(String.format("I am the thread '%s' and I am responsible for folder '%s'",
                                Thread.currentThread().getName(), folders.get(i)));
                        try {
                            System.out.println(ThreadName + ": Indexing to directory '" + partialDirs.get(i) + "'...");
                            indexDocs(writer, folders.get(i));
                            writer.close();

                        } catch (IOException e) {
                            System.out.println(ThreadName + ": caught a " + e.getClass() + "with message:" + e.getMessage());
                        }

                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private IndexFiles(KnnVectorDict vectorDict) throws IOException {
        if (vectorDict != null) {
            this.vectorDict = vectorDict;
            demoEmbeddings = new DemoEmbeddings(vectorDict);
        } else {
            this.vectorDict = null;
            demoEmbeddings = null;
        }
    }



    /** Index all text files under a directory. */


    /**
     * Indexes the given file using the given writer, or if a directory is given, recurses over files
     * and directories found under the given directory.
     *
     * <p>NOTE: This method indexes one document per input file. This is slow. For good throughput,
     * put multiple documents into your input file(s). An example of this is in the benchmark module,
     * which can create "line doc" files, one document per line, using the <a
     * href="../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
     * >WriteLineDocTask</a>.
     *
     * @param writer Writer to the index where the given file/dir info will be stored
     * @param path The file to index, or the directory to recurse into to find files to index
     * @throws IOException If there is a low-level I/O error
     */
    static void indexDocs(final IndexWriter writer, Path path) throws IOException, AccessDeniedException {

            if (Files.isDirectory(path)) {
                Files.walkFileTree(path, EnumSet.noneOf(FileVisitOption.class), deep, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        try {
                            indexDoc(writer, file, attrs.lastModifiedTime().toMillis());        //toMillis para saber cuanto tarda en indexar en milisegundos
                        } catch (@SuppressWarnings("unused")
                                IOException ignore) {
                            ignore.printStackTrace(System.err);
                            // don't index files that can't be read.
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            } else {
                indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
            }
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


        System.out.println(prop);

        String fileTypesList = prop.getProperty("onlyFiles");

        if (fileTypesList != null) {
            String[] fileTypesSplit = fileTypesList.split(" ");
            fileTypes.addAll(Arrays.asList(fileTypesSplit));
        } else
            System.out.println("Warning, no file types specified in config file");

        //Reading topLines property
        String onlyTopLines = prop.getProperty("onlyTopLines");
        if (onlyTopLines != null) {
            try {
                topLines = Integer.parseInt(onlyTopLines);
            } catch (Exception e) {
                System.out.println("Error reading onlyTopLines property " + e);
            }
        }

        //Reading bottomLines property
        String onlyBottomLines = prop.getProperty("onlyBottomLines");
        if (onlyBottomLines != null) {
            try {
                bottomLines = Integer.parseInt(onlyBottomLines);
            } catch (Exception e) {
                System.out.println("Error reading onlyBottomLines property " + e);
            }
        }
    }


    private static String readFile(String file) throws IOException {

        String result = "";
        BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(file)));
        for (String line; (line = br.readLine()) != null; )
            result += line + "\n";

        br.close();

        return result;
    }

    private static String readNLines(String file, int ntop, int nbot, String mode) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader(file));
        List<String> wantedLines = new ArrayList<>();
        String result = "";


        for (String line; (line = br.readLine()) != null; )
            wantedLines.add(line);

        br.close();

        if (mode == "top") {
            for (int i = 0; i < ntop; i++)
                if (wantedLines.size() != 0) {
                    result += wantedLines.get(0) + "\n";
                    wantedLines.remove(0);
                }
            return result;

        } else if (mode == "bot") {
            int limit = wantedLines.size() - nbot;
            for (int i = wantedLines.size() - 1; i >= limit; i--)
                if (wantedLines.size() != 0) {
                    result += wantedLines.get(wantedLines.size() - 1) + "\n";
                    wantedLines.remove(wantedLines.size() - 1);
                }
            return result;

        } else {
            for (int i = 0; i < ntop; i++)
                if (wantedLines.size() != 0) {
                    result += wantedLines.get(0) + "\n";
                    wantedLines.remove(0);
                }
            int limit = wantedLines.size() - nbot;
            for (int i = wantedLines.size() - 1; i >= limit; i--)
                if (wantedLines.size() != 0) {
                    result += wantedLines.get(wantedLines.size() - 1) + "\n";
                    wantedLines.remove(wantedLines.size() - 1);
                }
            return result;
        }

    }
    private static String getExtension(File file) {
        String fileName = file.getName();
        if (fileName.contains("."))
            return fileName.substring(fileName.indexOf("."));
        return null;
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

    /** Indexes a single document */
    static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
        if (fileTypes.isEmpty() || fileTypes.contains(getExtension(file.toFile()))) {
            try (InputStream stream = Files.newInputStream(file)) {
                // make a new, empty document
                Document doc = new Document();


                // Add the path of the file as a field named "path".  Use a
                // field that is indexed (i.e. searchable), but don't tokenize
                // the field into separate words and don't index term frequency
                // or positional information:

                Field pathField = new StringField("path", file.toString(), Field.Store.YES);
                doc.add(pathField);
                if (topLines != 0) {
                    if (bottomLines != 0)
                        doc.add(new Field("contents", readNLines(file.toString(), topLines, bottomLines, "topbot"), TYPE_STORED));
                    else
                        doc.add(new Field("contents", readNLines(file.toString(), topLines, bottomLines, "top"),TYPE_STORED));
                } else {
                    if (bottomLines != 0)
                        doc.add(new Field("contents", readNLines(file.toString(), topLines, bottomLines, "bot"), TYPE_STORED));
                    else
                        doc.add(new Field("contents", readFile(file.toString()), TYPE_STORED));
                }

                //ESTO SE PODRÍA QUITAR, YA QUE SE COMPRUEBA EN EL ULTIMO ELSE
                //if(topLines==0 && bottomLines==0){
                  //  doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));}

                doc.add(new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));
                doc.add(new StringField("thread", Thread.currentThread().getName(), Field.Store.YES));
                doc.add(new DoublePoint("sizeKb", (double) Files.size(file)));
                //doc.add(new StoredField("sizeKb", (double) Files.size(file)));

                BasicFileAttributeView basicView = Files.getFileAttributeView(file, BasicFileAttributeView.class);

                String creationTime = basicView.readAttributes().creationTime().toString();
                String lastAccessTime = basicView.readAttributes().lastAccessTime().toString();
                String lastModifiedTime = basicView.readAttributes().lastModifiedTime().toString();
                doc.add(new StringField("creationTime", creationTime, Field.Store.YES));
                doc.add(new StringField("lastAccessTime", lastAccessTime, Field.Store.YES));
                doc.add(new StringField("lastModifiedTime", lastModifiedTime, Field.Store.YES));

                String creationTimeLucene = DateTools.dateToString(new Date(basicView.readAttributes().creationTime().toMillis()), DateTools.Resolution.MINUTE);
                String lastAccessTimeLucene = DateTools.dateToString(new Date(basicView.readAttributes().lastAccessTime().toMillis()), DateTools.Resolution.MINUTE);
                String lastModifiedTimeLucene = DateTools.dateToString(new Date(basicView.readAttributes().lastModifiedTime().toMillis()), DateTools.Resolution.MINUTE);
                doc.add(new StringField("creationTimeLucene", creationTimeLucene, Field.Store.YES));
                doc.add(new StringField("lastAccessTimeLucene", lastAccessTimeLucene, Field.Store.YES));
                doc.add(new StringField("lastModifiedTimeLucene", lastModifiedTimeLucene, Field.Store.YES));

                // Add the last modified date of the file a field named "modified".
                // Use a LongPoint that is indexed (i.e. efficiently filterable with
                // PointRangeQuery).  This indexes to milli-second resolution, which
                // is often too fine.  You could instead create a number based on
                // year/month/day/hour/minutes/seconds, down the resolution you require.
                // For example the long value 2011021714 would mean
                // February 17, 2011, 2-3 PM.
                doc.add(new LongPoint("modified", lastModified));

                // Add the contents of the file to a field named "contents".  Specify a Reader,
                // so that the text of the file is tokenized and indexed, but not stored.
                // Note that FileReader expects the file to be in UTF-8 encoding.
                // If that's not the case searching for special characters will fail.
                //doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));

                if (demoEmbeddings != null) {
                    try (InputStream in = Files.newInputStream(file)) {
                        float[] vector =
                                demoEmbeddings.computeEmbedding(
                                        new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)));
                        doc.add(
                                new KnnVectorField("contents-vector", vector, VectorSimilarityFunction.DOT_PRODUCT));
                    }
                }

                if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
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
            }
        }
    }

    public static void indexNonPartial(IndexWriter writer) {
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        List<Path> docsPathAux = new ArrayList<>();
        ArrayList<Path>[] al = new ArrayList[numThreads];
        File files;
        ArrayList<File> ficheros = new ArrayList<>();
        File[] directorio;
        File[] auxFiles;

        for (int x = 0; x < docsPaths.size(); x++) {
            System.out.println(docsPaths.get(x));
            files = new File(String.valueOf(docsPaths.get(x)));
            ficheros.add(files);
        }
        for (int x = 0; x < ficheros.size(); x++) {

            directorio = ficheros.get(x).listFiles();

            if (directorio == null) {
                System.out.println(ficheros.get(x) + " no es un directorio");
                System.exit(-2);
            }


            for (int y = 0; y < directorio.length; y++) {
                auxFiles = directorio[y].listFiles();
                if (auxFiles != null) {
                    docsPathAux.add(Path.of(directorio[y].getPath()));
                }

            }
        }

        for (int i = 0; i < numThreads; i++) {
            al[i] = new ArrayList<>();
        }

        while (!docsPathAux.isEmpty()) {
            for (int i = 0; i < numThreads; i++) {
                if (docsPathAux.size() > 0 && docsPathAux.get(0) != null) {
                    al[i].add(docsPathAux.get(0));
                    docsPathAux.remove(0);
                }
            }
        }

        for (int i = 0; i < numThreads; i++) {

            final Runnable worker = new WorkerThread(al[i], writer);
            executor.execute(worker);
        }

        executor.shutdown();
        /* Wait up to 1 hour to finish all the previously submitted jobs */
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }
    }



    static void indexPartial (IndexWriter writer, String indexPath) throws IOException {
        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        File files;
        ArrayList<File> ficheros = new ArrayList<>();
        File[] directorio;
        List<Path> docsPathAux = new ArrayList<>();
        File[] auxFiles;
        ArrayList<Path>[] al = new ArrayList[numThreads];
        ArrayList<FSDirectory>[] alindex = new ArrayList[numThreads];
        int n = 2;
        int d = 1;
        for (int i = 0; i < numThreads; i++) {
            al[i] = new ArrayList<>();
            alindex[i] = new ArrayList<>();
        }


        for (int x = 0; x < docsPaths.size(); x++) {
            System.out.println(docsPaths.get(x));
            files = new File(String.valueOf(docsPaths.get(x)));
            ficheros.add(files);
        }
        for (int x = 0; x < ficheros.size(); x++) {

            directorio = ficheros.get(x).listFiles();

            if (directorio == null) {
                System.out.println(ficheros.get(x) + " no es un directorio");
                System.exit(-2);
            }


            for (int y = 0; y < directorio.length; y++) {
                auxFiles = directorio[y].listFiles();
                if (auxFiles != null) {
                    docsPathAux.add(Path.of(directorio[y].getPath()));
                }

            }
        }
        while (!docsPathAux.isEmpty()) {
            for (int i = 0; i < numThreads; i++) {
                if (docsPathAux.size() > 0 && docsPathAux.get(0) != null) {
                    if (d <= n) {
                        al[i].add(docsPathAux.get(0));
                        alindex[i].add(FSDirectory.open(Paths.get((docsPaths.get(0).toString() + "/tmp" + i))));
                        d++;
                        docsPathAux.remove(0);
                    }
                }
            }
        }
        for (int i = 0; i < numThreads; i++) {
            final Runnable worker = new WorkerThread(al[i], alindex[i]);
            executor.execute(worker);
        }
        executor.shutdown();
        /* Wait up to 1 hour to finish all the previously submitted jobs */
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }


        System.out.println("Merging indexes in " + indexPath);
        try {
            for (int j = 0; j < alindex.length; j++)
                for (int k = 0; k < alindex[j].size(); k++) {
                    writer.addIndexes(alindex[j].get(k));
                    deleteFile(alindex[j].get(k).getDirectory().toFile());
                }
        } catch (IOException e) {
            System.out.println("Error while merging: " + e);
        }





    }
    private static void deleteFile(File file) {// Eliminar carpetas de forma recursiva
        if (file.exists()) {// Determine si el archivo existe
            if (file.isFile()) {// Determinar si es un archivo
                file.delete();//Borrar archivos
                 }
                 else if (file.isDirectory()) {
                    // De lo contrario si es un directorio
                    File[] files = file.listFiles();// Declara todos los archivos del directorio files [];
                    for (int i = 0;i < files.length;i ++) {// Recorre todos los archivos del directorio
                        deleteFile(files[i]);// Usa este método para iterar cada archivo
                         }
                        file.delete();
                        //Eliminar carpeta
                        }
                 } else {
                     System.out.println("El archivo eliminado no existe");
                 }
            }

    @Override
    public void close() throws IOException {
        IOUtils.close(vectorDict);
    }
    public static void main(String[] args) throws Exception {

        String indexPath = "index";
        String docsPath = null;

        String vectorDictSource = null;
        Path docDir= null;
        String create = "create";

        int para=0;

        for (int i = 0; i < args.length; i++) {

            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    para=0;
                    break;
                case "-docs":
                    docsPath = args[++i];
                    docsPaths.add(Paths.get(docsPath));
                    para=1;
                    break;
                case "-knn_dict":
                    vectorDictSource = args[++i];
                    break;
                case "-update":
                    create = "create_or_append";
                    break;
                case "-numThreads":
                    numThreads = Integer.parseInt(args[++i]);
                    break;
                case "-create":
                    create = "create";
                    break;
                case "-partialIndexes":
                    partialIndex=true;
                    break;
                case "-openmode":
                    create = args[++i];
                    break;
                case "-deep":
                    deep = Integer.parseInt(args[++i]);
                    break;
                default:
                    if(para==1){
                        docDir = Paths.get(args[i]);
                        docsPaths.add(docDir);
                    }else
                        throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        readConfigFile(DEFAULT_PATH);


        if (docsPath == null) {
            System.exit(1);
        }

        for(int di=0; di< docsPaths.size(); di ++){
            if (!Files.isReadable(docsPaths.get(di))) {
                System.out.println(
                        "Document directory '"
                                + docDir.toAbsolutePath()
                                + "' does not exist or is not readable, please check the path");
                System.exit(1);
            }
        }

        Date start = new Date();
        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            Directory dir = FSDirectory.open(Paths.get(indexPath));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            switch (create) {
                case "create":
                    // Create a new index in the directory, removing any
                    // previously indexed documents:
                    iwc.setOpenMode(OpenMode.CREATE);
                    break;
                case "create_or_append":
                    // Create a new index in the directory, removing any
                    // previously indexed documents:
                    iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
                    break;
                case "append":
                    // Create a new index in the directory, removing any
                    // previously indexed documents
                    iwc.setOpenMode(OpenMode.APPEND);
                    break;
                default:
                    iwc.setOpenMode(OpenMode.CREATE);
            }

            // Optional: for better indexing performance, if you
            // are indexing many documents, increase the RAM
            // buffer.  But if you do this, increase the max heap
            // size to the JVM (eg add -Xmx512m or -Xmx1g):
            //
            // iwc.setRAMBufferSizeMB(256.0);

            KnnVectorDict vectorDictInstance = null;
            long vectorDictSize = 0;
            if (vectorDictSource != null) {
                KnnVectorDict.build(Paths.get(vectorDictSource), dir, KNN_DICT);
                vectorDictInstance = new KnnVectorDict(dir, KNN_DICT);
                vectorDictSize = vectorDictInstance.ramBytesUsed();
            }

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                // indexFiles.indexDocs(writer, docDir);

                if(partialIndex)
                    indexPartial(writer, indexPath);
                else
                    indexNonPartial(writer);

                // NOTE: if you want to maximize search performance,
                // you can optionally call forceMerge here.  This can be
                // a terribly costly operation, so generally it's only
                // worth it when your index is relatively static (ie
                // you're done adding documents to it):
                //
                // writer.forceMerge(1);
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
            System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
        }

    }
}