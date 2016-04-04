package parquet.writer.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.Log;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
import org.junit.Test;

import org.apache.parquet.example.data.Group;

public class NewWriter {
	private static final Log LOG = Log.getLog(NewWriter.class);

	public static final String CSV_DELIMITER= "|";

	private static String readFile(String path) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(path));
		StringBuilder stringBuilder = new StringBuilder();

		try {
			String line = null;
			String ls = System.getProperty("line.separator");

			while ((line = reader.readLine()) != null ) {
				stringBuilder.append(line);
				stringBuilder.append(ls);
			}
		} finally {
			Utils.closeQuietly(reader);
		}

		return stringBuilder.toString();
	}
	
	public static String getSchema(File csvFile) throws IOException {
		String fileName = csvFile.getName().substring(
				0, csvFile.getName().length() - ".csv".length()) + ".schema";
		File schemaFile = new File(csvFile.getParentFile(), fileName);
		return readFile(schemaFile.getAbsolutePath());
	}

	public static void convertCsvToParquet(File csvFile, File outputParquetFile) throws IOException {
		convertCsvToParquet(csvFile, outputParquetFile, false);
	}

	public static void convertCsvToParquet(File csvFile, File outputParquetFile, boolean enableDictionary) throws IOException {
		LOG.info("Converting " + csvFile.getName() + " to " + outputParquetFile.getName());
		String rawSchema = getSchema(csvFile);
		if(outputParquetFile.exists()) {
			throw new IOException("Output file " + outputParquetFile.getAbsolutePath() + 
					" already exists");
		}

		Path path = new Path(outputParquetFile.toURI());

		MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
		CsvParquetWriter writer = new CsvParquetWriter(path, schema, enableDictionary);

		BufferedReader br = new BufferedReader(new FileReader(csvFile));
		String line;
		int lineNumber = 0;
		try {
			while ((line = br.readLine()) != null) {
				String[] fields = line.split(Pattern.quote(CSV_DELIMITER));
				String[] compactFields = new String[fields.length];
				//for (int i = 0; i < fields.length; ++i) {
					//if(i == 1)
					//	compactFields[i] = compactString(fields[i]);
					//else
					//	compactFields[i] = fields[i];
				//}
				writer.write(Arrays.asList(fields));
				//writer.write(Arrays.asList(compactFields));
				++lineNumber;
			}

			writer.close();
		} finally {
			LOG.info("Number of lines: " + lineNumber);
			Utils.closeQuietly(br);
		} 
	}

	
	@Test
	public void test() throws IOException {
		File inFile = new File("/Users/LangYu/tmp/test.csv");
		File outFile = new File("/Users/LangYu/tmp/out.par");
		System.out.println("Convert");
		convertCsvToParquet(inFile, outFile);
		System.out.println("Succeed");
		return;
	}
	
	private static String compactString(String inString) {
		String rst = "";
		int counter = 0;
		byte tmp = 0x00;
		for (char c : inString.toCharArray()) {
			String buf = "";
			if (counter == 4) {
				rst += (char) tmp;
				//System.out.println("hi");
				counter = 0;
				tmp = 0x00;
			}
			switch (c) {
			case 'A':
				tmp = (byte) ((tmp << 2) + 0x00);
				counter++;
				break;
			case 'C':
				tmp = (byte) ((tmp << 2) + 0x01);
				counter++;
				break;
			case 'T':
				tmp = (byte) ((tmp << 2) + 0x02);
				counter++;
				break;
			case 'G':
				tmp = (byte) ((tmp << 2) + 0x03);
				counter++;
				break;
			default:
				System.out.println("Unknown character: " + c);
				break;
			}
		}

		if (counter != 0) {
			//System.out.println("counter is " + counter);
			rst += (char) (tmp << (2 * (4 - counter)));
		}
		return rst;
	}
}