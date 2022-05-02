package es.udc.fi.ri;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
public class BestTermsInColl {
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
    private static class IDFLOG10Comparator implements Comparator<TermValues> {

        public int compare(TermValues t1, TermValues  t2) {
            if (t1.idflog10 <  t2.idflog10)
                return -1;
            if ( t2.idflog10 < t1.idflog10)
                return 1;
            else
                return 0;
        }
    }
    private static class TermValues {
        public String term;
    
        public double df;
        public double idflog10;



    public  TermValues(String term, double df, double idflog10) {
            this.term=term;
            this.df = df;
            this.idflog10=idflog10;
        }
    }
    
    public static void main(String[] args) throws Exception {

        String indexPath = "index";
        String field = null;
        int top= 0;
        Boolean rev= false;
        DirectoryReader reader = null;


        for (int i = 0; i < args.length; i++) {

            switch (args[i]) {

                case "-index":
                    indexPath = args[++i];
                    break;
                case "-field":
                    field = args[++i];
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-rev":
                    rev = true;
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);

            }
        }
        try {
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        try {
            for(int i = 0; i<reader.maxDoc();i++) {
                Terms termVector;
                if ((termVector = reader.getTermVector(i,field)) == null) {
                    System.out.println("Document has no term vector");
                    System.exit(-1);
                }

                TermsEnum iterator = termVector.iterator();
                System.out.println("IndexPath: " + indexPath);
                reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
                BytesRef tmpTerm;
                ArrayList<TermValues> termValuesArray = new ArrayList<>();
                PostingsEnum docs = null;


                while ((tmpTerm = iterator.next()) != null) {
                    Term term = new Term(field, tmpTerm);
                    int df = reader.docFreq(term);
                    double idflog10 = 0;
                    docs = iterator.postings(docs, PostingsEnum.NONE);
                    while (docs.nextDoc() != PostingsEnum.NO_MORE_DOCS) {

                        termValuesArray.add(new TermValues(term.text(), df, idflog10));
                    }

                }


                System.out.printf("Best terms in: "+termValuesArray.get(i).term+"\n");
                if (rev==true){
                    termValuesArray.sort(  new DFComparator());
                System.out.printf("Best terms: DF\n");
                for (int j = 0; j <top && j<termValuesArray.size(); j++)
                    System.out.println(termValuesArray.get(j).df+ "\n");}
                else{
                    termValuesArray.sort(  new IDFLOG10Comparator());
                    System.out.printf("Best terms: IDFLOG\n");
                    for (int j = 0; j < top && j<termValuesArray.size() ; j++)
                        System.out.println(termValuesArray.get(j).idflog10+ "\n");}



            } } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
