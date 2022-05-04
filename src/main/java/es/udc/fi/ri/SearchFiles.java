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
import org.apache.lucene.demo.knn.DemoEmbeddings;
import org.apache.lucene.demo.knn.KnnVectorDict;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.FSDirectory;

/** Simple command-line based search demo. */
public class SearchFiles {

	static HashMap<Integer, String> queries = new HashMap<>();
	static final String ALL_QUERIES = "1-93";
	static final Path QUERIES_PATH = Paths.get("");
	static final Path RELEVANCE_PATH = Paths.get("");
	static Path queryFile = QUERIES_PATH;

	private SearchFiles() {
	}
	public static HashMap<Integer, String> findQuery(String n) throws IOException {
		try (InputStream stream = Files.newInputStream(queryFile)) {
			String line;
			HashMap<Integer, String> result = new HashMap<>();
			BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
			while ((line = br.readLine()) != null) {
				if (line.equals(n)){
					result.put(Integer.parseInt(n), br.readLine());
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
		String queries = null;
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
				queries = args[++i];
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
				break;

			case "-search":
				indexingmodel = args[++i];
				if (indexingmodel.equals("jm"))
					lambda = Float.valueOf(args[++i]);
				break;
				break;

			default:
				System.err.println("Unknown argument: " + args[i]);
				System.exit(1);
			}
		}

		DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));
		IndexSearcher searcher = new IndexSearcher(reader);
		Analyzer analyzer = new StandardAnalyzer();
		KnnVectorDict vectorDict = null;
		if (knnVectors > 0) {
			vectorDict = new KnnVectorDict(reader.directory(), IndexMedline.KNN_DICT);
		}
		BufferedReader in;
		if (queries != null) {
			in = Files.newBufferedReader(Paths.get(queries), StandardCharsets.UTF_8);
		} else {
			in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
		}
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


			System.out.println("Searching for: " + query.toString(field));
		for (Map.Entry<Integer, String> entry : queries.entrySet()) {
			int num = entry.getKey();
			String line = entry.getValue();
			line = line.trim();
			Query query = parser.parse(line);
			System.out.println("Searching for: " + query.toString(field));
			doPagingSearch(searcher, query, num);
		}
		/*if (knnVectors > 0) {
			query = addSemanticQuery(query, vectorDict, knnVectors);
		}*/
		float sum = 0;
		/*for (Float d : metrics)
			sum += d;
		System.out.println("Mean of the metric for all the queries = " + (float) sum / queryCount);*/


		reader.close();
	}
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

	public static void doPagingSearch(BufferedReader in, IndexSearcher searcher, Query query, int hitsPerPage,
			boolean raw, boolean interactive) throws IOException {

		// Collect enough docs to show 5 pages
		TopDocs results = searcher.search(query, 5 * hitsPerPage);
		ScoreDoc[] hits = results.scoreDocs;

		int numTotalHits = Math.toIntExact(results.totalHits.value);
		System.out.println(numTotalHits + " total matching documents");

		int start = 0;
		int end = Math.min(numTotalHits, hitsPerPage);

		while (true) {
			if (end > hits.length) {
				System.out.println("Only results 1 - " + hits.length + " of " + numTotalHits
						+ " total matching documents collected.");
				System.out.println("Collect more (y/n) ?");
				String line = in.readLine();
				if (line.length() == 0 || line.charAt(0) == 'n') {
					break;
				}

				hits = searcher.search(query, numTotalHits).scoreDocs;
			}

			end = Math.min(hits.length, start + hitsPerPage);

			for (int i = start; i < end; i++) {
				if (raw) { // output raw format
					System.out.println("doc=" + hits[i].doc + " score=" + hits[i].score);
					continue;
				}

				Document doc = searcher.doc(hits[i].doc);
				String path = doc.get("path");
				if (path != null) {
					System.out.println((i + 1) + ". " + path);
					String title = doc.get("title");
					if (title != null) {
						System.out.println("   Title: " + doc.get("title"));
					}
				} else {
					System.out.println((i + 1) + ". " + "No path for this document");
				}
			}

			if (!interactive || end == 0) {
				break;
			}

			if (numTotalHits >= end) {
				boolean quit = false;
				while (true) {
					System.out.print("Press ");
					if (start - hitsPerPage >= 0) {
						System.out.print("(p)revious page, ");
					}
					if (start + hitsPerPage < numTotalHits) {
						System.out.print("(n)ext page, ");
					}
					System.out.println("(q)uit or enter number to jump to a page.");

					String line = in.readLine();
					if (line.length() == 0 || line.charAt(0) == 'q') {
						quit = true;
						break;
					}
					if (line.charAt(0) == 'p') {
						start = Math.max(0, start - hitsPerPage);
						break;
					} else if (line.charAt(0) == 'n') {
						if (start + hitsPerPage < numTotalHits) {
							start += hitsPerPage;
						}
						break;
					} else {
						int page = Integer.parseInt(line);
						if ((page - 1) * hitsPerPage < numTotalHits) {
							start = (page - 1) * hitsPerPage;
							break;
						} else {
							System.out.println("No such page");
						}
					}
				}
				if (quit)
					break;
				end = Math.min(numTotalHits, start + hitsPerPage);
			}
		}
	}

	private static Query addSemanticQuery(Query query, KnnVectorDict vectorDict, int k) throws IOException {
		StringBuilder semanticQueryText = new StringBuilder();
		QueryFieldTermExtractor termExtractor = new QueryFieldTermExtractor("contents");
		query.visit(termExtractor);
		for (String term : termExtractor.terms) {
			semanticQueryText.append(term).append(' ');
		}
		if (semanticQueryText.length() > 0) {
			KnnVectorQuery knnQuery = new KnnVectorQuery("contents-vector",
					new DemoEmbeddings(vectorDict).computeEmbedding(semanticQueryText.toString()), k);
			BooleanQuery.Builder builder = new BooleanQuery.Builder();
			builder.add(query, BooleanClause.Occur.SHOULD);
			builder.add(knnQuery, BooleanClause.Occur.SHOULD);
			return builder.build();
		}
		return query;
	}

	private static class QueryFieldTermExtractor extends QueryVisitor {
		private final String field;
		private final List<String> terms = new ArrayList<>();

		QueryFieldTermExtractor(String field) {
			this.field = field;
		}

		@Override
		public boolean acceptField(String field) {
			return field.equals(this.field);
		}

		@Override
		public void consumeTerms(Query query, Term... terms) {
			for (Term term : terms) {
				this.terms.add(term.text());
			}
		}

		@Override
		public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
			if (occur == BooleanClause.Occur.MUST_NOT) {
				return QueryVisitor.EMPTY_VISITOR;
			}
			return this;
		}
	}
}