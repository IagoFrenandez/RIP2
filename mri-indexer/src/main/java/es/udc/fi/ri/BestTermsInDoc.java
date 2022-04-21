package es.udc.fi.ri;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
public class BestTermsInDoc {
    private static class TermValues {
        public String term;
        public double tf;
        public double df;
        public double idf;
        public double tfxidf;
        public double idflog10;
        public double tfxidflog10;

        public TermValues(String term, double tf, double df, double idf,double idflog10) {
            this.term = term;
            this.tf = tf;
            this.df = df;
            this.idf = idf;
            this.idflog10=idflog10;
            this.tfxidf = tf * idf;
            this.tfxidflog10=idflog10 * tf;
        }
    }

    private static class TFXIDFComparator implements Comparator<TermValues> {

        public int compare(TermValues t1, TermValues t2) {
            if (t1.tfxidf < t2.tfxidf)
                return -1;
            if (t2.tfxidf < t1.tfxidf)
                return 1;
            else
                return 0;
        }
    }

    private static class TFComparator implements Comparator<TermValues> {

        public int compare(TermValues t1, TermValues t2) {
            if (t1.tf < t2.tf)
                return -1;
            if (t2.tf < t1.tf)
                return 1;
            else
                return 0;
        }
    }

    private static class DFComparator implements Comparator<TermValues> {

        public int compare(TermValues t1, TermValues t2) {
            if (t1.df < t2.df)
                return -1;
            if (t2.df < t1.df)
                return 1;
            else
                return 0;
        }
    }

    private static class IDFComparator implements Comparator<TermValues> {

        public int compare(TermValues t1, TermValues t2) {
            if (t1.idf < t2.idf)
                return -1;
            if (t2.idf < t1.idf)
                return 1;
            else
                return 0;
        }
    }
    private static class IDFLOG10Comparator implements Comparator<TermValues> {

        public int compare(TermValues t1, TermValues t2) {
            if (t1.idflog10 < t2.idflog10)
                return -1;
            if (t2.idflog10 < t1.idflog10)
                return 1;
            else
                return 0;
        }
    }
    private static class IDFLOG10XTFComparator implements Comparator<TermValues> {

        public int compare(TermValues t1, TermValues t2) {
            if (t1.tfxidflog10 < t2.tfxidflog10)
                return -1;
            if (t2.tfxidflog10 < t1.tfxidflog10)
                return 1;
            else
                return 0;
        }
    }

    public static void writeonscreen(String order,ArrayList<TermValues> termValuesArray,int top){
        switch (order) {
            case "tf":
                termValuesArray.sort( new TFComparator());
                System.out.printf("Best terms: TF\n");
                for (int i = 0; i < top  && i<termValuesArray.size(); i++)
                    System.out.println("TF: "+ termValuesArray.get(i).tf+" DF: "+ termValuesArray.get(i).df+" IDFLOG10: "+termValuesArray.get(i).idflog10+" TF X IDFLOG10 "+termValuesArray.get(i).tfxidflog10 +"\n");
                break;
            case "df":
                termValuesArray.sort( new DFComparator());
                System.out.printf("Best terms: DF\n");
                for (int i = 0; i < top  && i<termValuesArray.size(); i++){
                    System.out.printf("Esparrago\n");
                    System.out.println("TF: "+ termValuesArray.get(i).tf+" DF: "+ termValuesArray.get(i).df+" IDFLOG10: "+termValuesArray.get(i).idflog10+" TF X IDFLOG10 "+termValuesArray.get(i).tfxidflog10 +"\n");}
                break;
            case "tfxidf":
                termValuesArray.sort( new TFXIDFComparator());
                System.out.printf("Best terms: TFIDF\n");
                for (int i = 0; i < top  && i<termValuesArray.size(); i++)
                    System.out.println(" TF x IDFLOG10: "+ termValuesArray.get(i).tfxidf+" TF: "+ termValuesArray.get(i).tf+" DF: "+ termValuesArray.get(i).df+" IDFLOG10: "+termValuesArray.get(i).idflog10+" TF X IDFLOG10 "+termValuesArray.get(i).tfxidflog10 +"\n");
                break;
            case "idf":
                termValuesArray.sort( new IDFComparator());
                System.out.printf("Best terms: IDF\n");
                for (int i = 0; i < top  && i<termValuesArray.size(); i++)
                    System.out.println(" IDF: "+ termValuesArray.get(i).idf+" TF: "+ termValuesArray.get(i).tf+" DF: "+ termValuesArray.get(i).df+" IDFLOG10: "+termValuesArray.get(i).idflog10+" TF X IDFLOG10 "+termValuesArray.get(i).tfxidflog10 +"\n");
                break;
           /* case "-IDFLOG10":
                Collections.sort(termValuesArray, new IDFLOG10Comparator());
                System.out.printf("Best terms: IDFLOG10\n");
                for (int i = 0; i < top  && i<termValuesArray.size(); i++)
                    System.out.println(termValuesArray.get(i).term);
                break;
            case "-TFXIDFLOG10":
                Collections.sort(termValuesArray, new IDFLOG10XTFComparator());
                System.out.printf("Best terms: TFXIDFLOG10\n");
                for (int i = 0; i < top  && i<termValuesArray.size(); i++)
                    System.out.println(termValuesArray.get(i).term);
                break;*/
            default:
                throw new IllegalArgumentException("unknown parameter " + order);
        }




    }
    public static void writeindocument(String order,String outputfile,ArrayList<TermValues> termValuesArray,int top) throws IOException {

        BufferedWriter bw = new BufferedWriter(new FileWriter(outputfile));
        switch (order) {
            case "tf":
                termValuesArray.sort( new TFComparator());
                bw.write("Best terms: TF\n");
                for (int i = 0; i < top && i<termValuesArray.size(); i++)
                 bw.write(termValuesArray.get(i).term+ "\n");
                break;
            case "df":
                termValuesArray.sort( new DFComparator());
                bw.write("Best terms: DF\n");
                for (int i = 0; i < top  && i<termValuesArray.size(); i++)
                    bw.write(termValuesArray.get(i).df+ "\n");
                break;
            case "tfxidf":
                termValuesArray.sort( new TFXIDFComparator());
                bw.write("Best terms: TFIDF\n");
                for (int i = 0; i < top  && i<termValuesArray.size(); i++)
                    bw.write(termValuesArray.get(i).tfxidf+ "\n");
                break;
            case "idf":
                termValuesArray.sort( new IDFComparator());
                bw.write("Best terms: IDF\n");
                for (int i = 0; i < top  && i<termValuesArray.size(); i++)
                    bw.write(termValuesArray.get(i).idf+ "\n");
                break;
           /* case "-IDFLOG10":
                Collections.sort(termValuesArray, new IDFLOG10Comparator());
                System.out.printf("Best terms: IDFLOG10\n");
                for (int i = 0; i < top; i++)
                    System.out.println(termValuesArray.get(i).term);
                break;
            case "-TFXIDFLOG10":
                Collections.sort(termValuesArray, new IDFLOG10XTFComparator());
                System.out.printf("Best terms: TFXIDFLOG10\n");
                for (int i = 0; i < top; i++)
                    System.out.println(termValuesArray.get(i).term);
                break;*/
            default:
                throw new IllegalArgumentException("unknown parameter " + order);
        }




    }
    public static void main(String[] args) throws Exception {
         String indexPath = "index";
         int docID=0;
         String field=null;
         int top=0;
         String order=null;
         String outputfile = null;
         Boolean outputfilecontrol= false;

        for (int i = 0; i < args.length; i++) {

            switch (args[i]) {

                case "-index":
                    indexPath = args[++i];
                    break;
                case "-field":
                    field = args[++i];
                    break;
                case "-docID":
                    docID = Integer.parseInt(args[++i]);
                    break;
                case "-order":
                    order = args[++i];
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-outpufile":
                    outputfile = args[++i];
                    outputfilecontrol=true;
                    break;


                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);

            }
        }

        DirectoryReader reader = null;

        try {
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        Terms termVector;

        if ((termVector = reader.getTermVector(docID, field)) == null) {
            System.out.println("Document has no term vector");
            System.exit(-1);
        }
        PostingsEnum docs = null;
        TermsEnum iterator = termVector.iterator();
        int docsCount = reader.numDocs();
        TFIDFSimilarity classicSimilarity = new ClassicSimilarity();
        ArrayList<TermValues> termValuesArray = new ArrayList<>();
        BytesRef tmpTerm;

        while ((tmpTerm = iterator.next()) != null) {

            Term term = new Term(field, tmpTerm);
            long indexDf = reader.docFreq(term);
            double idf = classicSimilarity.idf(indexDf,docsCount );
            int df = reader.docFreq(term);

            docs = iterator.postings(docs, PostingsEnum.NONE);


            while (docs.nextDoc() != PostingsEnum.NO_MORE_DOCS) {

                double tf = classicSimilarity.tf(docs.freq());
                double idflog10 = Math.log10(docsCount/tf);
                termValuesArray.add(new TermValues(term.text(), tf, df, idf,idflog10));
            }

        }


        if (outputfilecontrol==true)
            writeindocument(order,outputfile,termValuesArray,top);
            else
            writeonscreen(order,termValuesArray,top);




    }
}
