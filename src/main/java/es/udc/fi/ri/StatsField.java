package es.udc.fi.ri;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;


import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.store.FSDirectory;



public class StatsField {

    static String gatherStatistics(IndexReader reader, IndexableField field) throws IOException{

        if(reader.getDocCount(field.name())>0) {

            CollectionStatistics collectionStats = new CollectionStatistics(
                    field.name(),
                    reader.maxDoc(),
                    reader.getDocCount(field.name()),
                    reader.getSumTotalTermFreq(field.name()),
                    reader.getSumDocFreq(field.name())
            );

            if (collectionStats==null)
                return "No existe";
            else
                return collectionStats.toString();
        }
        return null;
    }


    public static void main(String[] args) throws Exception {

        String indexPath = "index";
        String field = null;

        for (int i = 0; i < args.length; i++) {

            switch (args[i]) {

                case "-index":
                    indexPath = args[++i];
                    break;
                case "-field":
                    field = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);

            }
        }

        IndexReader reader;
        Set<String> st = new HashSet<String>();

        try {
            System.out.println("IndexPath: "+indexPath);
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
            if(field==null) {
                for(int i = 0; i<reader.maxDoc();i++)

                    for(IndexableField ifield : reader.document(i).getFields()) {

                        String statistic=gatherStatistics(reader,ifield);
                        if(statistic!=null)
                            st.add(statistic);
                    }

            }
            if(!st.isEmpty()) {
                for(String statistic : st) {
                    System.out.println(statistic);
                }
            }
            reader.close();

        } catch (IOException e) {

            e.printStackTrace();
        }
    }
}

