package es.udc.fi.ri;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


public class WriteIndex {


    public static void main(String[] args) throws IOException {

        String indexPath = "index";
        String outputfile = "WriteIndex.txt";


        for (int i = 0; i < args.length; i++) {

            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-outputfile":
                    outputfile = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }


        Directory dir = null;
        DirectoryReader indexReader = null;
        Document doc = null;

        BufferedWriter bw = new BufferedWriter(new FileWriter(outputfile));

        List<IndexableField> fields;

        List<String> fieldSet = new ArrayList<>();

        try {
            dir = FSDirectory.open(Paths.get(indexPath));
            indexReader = DirectoryReader.open(dir);
        } catch (Exception e) {
            System.out.println("Couldn't read path: " + indexPath);
            e.printStackTrace();
        }

        bw.write("- Fields avaliable in Index: " + indexPath + "\n\n");

        for (int i = 0; i < indexReader.numDocs(); i++) {

            try {
                doc = indexReader.document(i);
            } catch (CorruptIndexException e1) {
                System.out.println("Graceful message: exception " + e1);
                e1.printStackTrace();
            } catch (IOException e1) {
                System.out.println("Graceful message: exception " + e1);
                e1.printStackTrace();
            }

            fields = doc.getFields();


            for (IndexableField field : fields) {

                String fieldName = field.name();

                if (!fieldSet.contains(fieldName)) {
                    fieldSet.add(fieldName);
                    bw.write(fieldName + "\n");
                    bw.write(field+ "\n");
                }
            }
        }

        try {
            indexReader.close();
            dir.close();
            bw.close();
        } catch (IOException e) {
            System.out.println("Graceful message: exception " + e);
            e.printStackTrace();
        }
    }
}
