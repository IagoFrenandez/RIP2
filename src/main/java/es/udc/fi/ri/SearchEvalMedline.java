package es.udc.fi.ri;
/*
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.FSDirectory;

/** Simple command-line based search demo. */
public class SearchEvalMedline {

	static HashMap<Integer, String> queries = new HashMap<>();
	static final String ALL_QUERIES = "1-30";
	static final Path QUERIES_PATH = Paths.get("C:\\Users\\iagof\\Desktop\\RI\\med.tar\\MED.QRY");
	static final Path RELEVANCE_PATH = Paths.get("C:\\Users\\iagof\\Desktop\\RI\\med.tar\\MED.REL");
	static Path queryFile = QUERIES_PATH;
	static int queryCount=0;
	static int queryMode =0;
	static String queryRange = "1-3";
	static String queryNum = "3";

	private SearchEvalMedline() {
	}
	public static HashMap<Integer, String> findQuery(String n) throws IOException {
		try (InputStream stream = Files.newInputStream(queryFile)) {
			String line;
			String aux = "";
			int m= Integer.parseInt(n)+1;
			System.out.println(m);
			HashMap<Integer, String> result = new HashMap<>();
			BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
			while ((line = br.readLine()) != null) {
				if (line.equals(".I " + n)  ) {
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
		System.out.println("Guisante");
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
	/** Simple command-line based search demo. */
	public static void main(String[] args) throws Exception {
		String usage = "Usage:\tjava org.apache.lucene.demo.SearchFiles [-index dir] [-field f] [-repeat n] [-queries file] [-query string] [-raw] [-paging hitsPerPage] [-knn_vector knnHits]\n\nSee http://lucene.apache.org/core/9_0_0/demo/ for details.";
		if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))) {
			System.out.println(usage);
			System.exit(0);
		}
		String indexingmodel = "tfidf";
		String index = "index";
		String field = "contents";
		List<Float> metrics = new ArrayList<>();
		float lambda = 0.5f;
		int cut = 1;
		int top = 1;
		boolean raw = false;
		int knnVectors = 0;
		String queryString = null;
		int hitsPerPage = 10;

		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
			case "-indexin":
				index = args[++i];
				break;
			/*case "-field":
				field = args[++i];
				break;*/
			case "-queries":
				if(args[i+1].equals("all")){
					queryMode = 0;	//si queryMode = 0, buscamos todas las queries
					queryRange = ALL_QUERIES;
					i++;
				}else if(args[i+1].contains("-")){
					queryMode = 1; //Buscamos un rango de queries
					queryRange = args[i+1];
					i++;
				}else {
					queryMode = 2; //Buscamos una query concreta
					queryNum = args[i+1];
					i++;
				}
				break;
			case "-top":
				top = Integer.parseInt(args[++i]);
				break;
			case "-cut":
				cut = Integer.parseInt(args[++i]);
				if (cut <= 0) {
					System.err.println("There must be at least 1 hit per page.");
					System.exit(1);
				}
				break;

			case "-search":
				indexingmodel = args[++i];
				if (indexingmodel.equals("jm"))
					lambda = Float.valueOf(args[++i]);
				break;

			default:
				System.err.println("Unknown argument: " + args[i]);
				System.exit(1);
			}
		}

		DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));
		IndexSearcher searcher = new IndexSearcher(reader);
		Analyzer analyzer = new StandardAnalyzer();

		QueryParser parser = new QueryParser(field, analyzer);
		switch (indexingmodel) {
			case "jm":
				searcher.setSimilarity(new LMJelinekMercerSimilarity(lambda));
				break;
			case "tfidf":
				searcher.setSimilarity(new ClassicSimilarity());
				break;
			default:
				searcher.setSimilarity(new ClassicSimilarity());
				break;
		}
		System.out.println("Cebolla "+queryRange);
		if(queryMode == 0){
			System.out.println("Zanahoria");
			queries.putAll(findQueries(queryRange));
		}else{
			System.out.println("Lechuga");
			queries.putAll(findQuery(queryNum));
		}

		System.out.println("Tomate "+ queries.toString());
		for
			(Map.Entry<Integer, String> entry : queries.entrySet()) {
				System.out.println("Remolacha");
				int num = entry.getKey();
				String line = entry.getValue();
				line = line.trim();
				Query query = parser.parse(QueryParser.escape(line));
				System.out.println("Searching for: " + query.toString(field));
				doPagingSearch(searcher, query, num,cut,top,metrics);
		}
		/*if (knnVectors > 0) {
			query = addSemanticQuery(query, vectorDict, knnVectors);
		}*/
		float sum = 0;
		for (Float d : metrics)
			sum += d;
		System.out.println("Mean of the metric for all the queries = " + (float) sum / queryCount);


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

	public static void doPagingSearch(IndexSearcher searcher, Query query, int num, int cut, int top, List<Float> metrics) throws IOException {
		TopDocs results = searcher.search(query, cut);
		ScoreDoc[] hits = results.scoreDocs;
		List<Integer> relevantDocs = findRelevantDocs(RELEVANCE_PATH, num);
		Set<Integer> relevantSet = new HashSet<>();

		List<Float> accumPrecision = new ArrayList<>();

		System.out.println("RELEVANT DOCS = " + relevantDocs.toString());
		System.out.println();
		int numTotalHits = Math.toIntExact(results.totalHits.value);
		System.out.println(numTotalHits + " total matching documents");

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
		end = Math.min(numTotalHits, top);
		for (int i = 0; i < end; i++) {
			Document doc = searcher.doc(hits[i].doc);
			int id = Integer.parseInt(doc.get("DocIDMedline"));
			System.out.println((i + 1) + ". Doc ID: " + id + " score = " + hits[i].score);
		}
		System.out.println();
				//P
				System.out.println("Precision at " + cut + " = " + (float) relevantSet.size() / cut);
				metrics.add((float) relevantSet.size() / cut);
				//R
				System.out.println("Recall at " + cut + " = " + (float) relevantSet.size() / relevantDocs.size());
				metrics.add((float) relevantSet.size() / relevantDocs.size());

				//MAP
				if (relevantSet.size() != 0) {
					float sum = 0;
					for (Float d : accumPrecision)
						sum += d;
					System.out.println("Mean Average Precision at " + cut + " = " + (float) sum / relevantSet.size());
					metrics.add((float) sum / relevantSet.size());
				} else
					System.out.println("Can't compute Mean Average Precision at " + cut + ", no relevant documents found");



		System.out.println();
		queryCount++;
	}

}