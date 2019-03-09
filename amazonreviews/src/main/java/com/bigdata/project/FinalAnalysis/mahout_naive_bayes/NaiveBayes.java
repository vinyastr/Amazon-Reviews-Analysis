package com.bigdata.project.FinalAnalysis.mahout_naive_bayes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;
import org.apache.mahout.vectorizer.TFIDF;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;

public class NaiveBayes {

	Configuration configuration = new Configuration();
	String inputFilePath, sequenceFilePath, labelIndexPath, modelPath, vectorsPath, dictionaryPath,
			documentFrequencyPath;

	public NaiveBayes(String pathToTrainFile) {
		String parentFolder = pathToTrainFile.substring(0, pathToTrainFile.lastIndexOf("/"));
		this.inputFilePath = pathToTrainFile;
		this.sequenceFilePath = parentFolder + "/reviews-seq";
		this.labelIndexPath = parentFolder + "/labelindex";
		this.modelPath = parentFolder + "/model";
		this.vectorsPath = parentFolder + "/review-vectors";
		this.dictionaryPath = parentFolder + "/review-vectors/dictionary.file-0";
		this.documentFrequencyPath = parentFolder + "/review-vectors/df-count/part-r-00000";

	}

	public void trainModel() throws Exception {
		inputDataToSequenceFile();
		sequenceFileToSparseVector();
		trainNaiveBayesModel();
	}

	private void inputDataToSequenceFile() throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader(inputFilePath));
		FileSystem fs = FileSystem.getLocal(configuration);
		Path seqFilePath = new Path(sequenceFilePath);
		fs.delete(seqFilePath, false);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, configuration, seqFilePath, Text.class, Text.class);
		int count = 0;
		try {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] tokens = line.split("\t");
				writer.append(new Text("/" + tokens[0] + "/review" + count++), new Text(tokens[1]));
			}
		} finally {
			reader.close();
			writer.close();
		}
	}

	private void sequenceFileToSparseVector() throws Exception {
		SparseVectorsFromSequenceFiles svfsf = new SparseVectorsFromSequenceFiles();
		svfsf.run(new String[] { "-i", sequenceFilePath, "-o", vectorsPath, "-ow" });
	}

	private void trainNaiveBayesModel() throws Exception {
		TrainNaiveBayesJob trainNaiveBayes = new TrainNaiveBayesJob();
		trainNaiveBayes.setConf(configuration);
		trainNaiveBayes.run(new String[] { "-i", vectorsPath + "/tfidf-vectors", "-o", modelPath, "-li", labelIndexPath,
				"-el", "-c", "-ow" });
	}

	/**
	 * This method returns a 1 for positive review or a 0 for negative review.
	 * 
	 * @param review:
	 *            The review that should be classified.
	 * @throws IOException
	 */
	public int classifyNewReview(String review) throws IOException {
		// System.out.println("review: " + review);

		Map<String, Integer> dictionary = readDictionary(configuration, new Path(dictionaryPath));
		Map<Integer, Long> documentFrequency = readDocumentFrequency(configuration, new Path(documentFrequencyPath));

		Multiset<String> words = ConcurrentHashMultiset.create();

		// Extract the words from the new review using Lucene
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
		TokenStream tokenStream = analyzer.tokenStream("text", new StringReader(review));
		CharTermAttribute termAttribute = tokenStream.addAttribute(CharTermAttribute.class);
		tokenStream.reset();
		int wordCount = 0;
		while (tokenStream.incrementToken()) {
			if (termAttribute.length() > 0) {
				String word = tokenStream.getAttribute(CharTermAttribute.class).toString();
				Integer wordId = dictionary.get(word);
				// If the word is not in the dictionary, skip it
				if (wordId != null) {
					words.add(word);
					wordCount++;
				}
			}
		}
		tokenStream.end();
		tokenStream.close();

		int documentCount = documentFrequency.get(-1).intValue();

		// Create a vector for the new review (wordId => TFIDF weight)
		Vector vector = new RandomAccessSparseVector(10000);
		TFIDF tfidf = new TFIDF();
		for (Multiset.Entry<String> entry : words.entrySet()) {
			String word = entry.getElement();
			int count = entry.getCount();
			Integer wordId = dictionary.get(word);
			Long freq = documentFrequency.get(wordId);
			double tfIdfValue = tfidf.calculate(count, freq.intValue(), wordCount, documentCount);
			vector.setQuick(wordId, tfIdfValue);
		}

		// Model is a matrix (wordId, labelId) => probability score
		NaiveBayesModel model = NaiveBayesModel.materialize(new Path(modelPath), configuration);
		StandardNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(model);

		// With the classifier, we get one score for each label.The label with
		// the highest score is the one the review is more likely to be
		// associated to
		Vector resultVector = classifier.classifyFull(vector);
		double bestScore = -Double.MAX_VALUE;
		int bestCategoryId = -1;
		for (Element element : resultVector.all()) {
			int categoryId = element.index();
			double score = element.get();
			if (score > bestScore) {
				bestScore = score;
				bestCategoryId = categoryId;
			}
//			if (categoryId == 1) {
//				 System.out.println("Probability of being positive: " +
//				 score);
//			} else {
//				 System.out.println("Probability of being negative: " +
//				 score);
//			}
		}
		analyzer.close();
		if (bestCategoryId == 1) {
			return 1;
		} else {
			return 0;
		}
	}

	private static Map<String, Integer> readDictionary(Configuration conf, Path dictionnaryPath) {
		Map<String, Integer> dictionnary = new HashMap<String, Integer>();
		for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(dictionnaryPath, true, conf)) {
			dictionnary.put(pair.getFirst().toString(), pair.getSecond().get());
		}
		return dictionnary;
	}

	private static Map<Integer, Long> readDocumentFrequency(Configuration conf, Path documentFrequencyPath) {
		Map<Integer, Long> documentFrequency = new HashMap<Integer, Long>();
		for (Pair<IntWritable, LongWritable> pair : new SequenceFileIterable<IntWritable, LongWritable>(
				documentFrequencyPath, true, conf)) {
			documentFrequency.put(pair.getFirst().get(), pair.getSecond().get());
		}
		return documentFrequency;
	}
}