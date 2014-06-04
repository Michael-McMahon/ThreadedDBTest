package testrunner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ResultsWriter {
	// Name of file to write to
	private String fileName;
	// Character sequence which will delimit rows
	private String rowDlm = "\n";
	// Character sequence which will delimit columns
	private String colDlm = ",";

	/**
	 * Creates a new results writer which will write results to the specified
	 * file. Uses default row and column delimiters.
	 * 
	 * @param fileName
	 *            Name of file to write results to. If the file does not exist,
	 *            then a new file will be created with the specified name.
	 */
	public ResultsWriter(String fileName) {
		this(fileName, "\n", ",");
	}

	/**
	 * Creates a new results writer which will write results to the specified
	 * file. Uses supplied row and column delimiters.
	 * 
	 * @param fileName
	 *            Name of file to write results to. If the file does not exist,
	 *            then a new file will be created with the specified name.
	 * @param rowDelimiter
	 *            The character(s) which will delimit a new row
	 * @param colDelimiter
	 *            The character(s) which will delimit a new column
	 */
	public ResultsWriter(String fileName, String rowDelimiter,
			String colDelimiter) {
		this.fileName = fileName;
		rowDlm = rowDelimiter;
		colDlm = colDelimiter;
	}

	/**
	 * Write an entry to the results file. Entry will consist of supplied values
	 * delimited by the column delimiter. Entry will end with the row delimiter.
	 * 
	 * @param values
	 *            The values to which will be written to an entry.
	 */
	public boolean writeResults(String... values) {
		int i;
		int len = values.length;

		// If no values are supplied, don't write anything
		if (len < 1) {
			return true;
		}

		// Get file writer
		BufferedWriter writer = getWriter();
		if (writer == null) {
			return false;
		}

		// Append first value with no column delimiter
		StringBuilder sb = new StringBuilder();
		sb.append(quoteDelimiter(values[0]));

		// Append all remaining values preceded with the column delimiter
		for (i = 1; i < len; i++) {
			sb.append(colDlm).append(quoteDelimiter(values[i]));
		}
		// Append final row delimiter
		sb.append(rowDlm);

		// Write the appended row to file
		try {
			writer.write(sb.toString());
		} catch (IOException e) {
			System.err.println("Failed to write string to file. "
					+ "String had value:" + sb.toString());
			e.printStackTrace();
			return false;
		} finally {
			try {
				writer.close();
			} catch (IOException e) {
				System.err
						.println("Failed to close writer for file with name: "
								+ fileName);
				e.printStackTrace();
				return false;
			}
		}

		return true;
	}

	/**
	 * Helper method to handle values which contain a column or row delimiter.
	 * Values containing a delimiter will be enclosed with double quotes.
	 * 
	 * @param val
	 *            String to be quoted if it contains a delimiter.
	 * @return A quote enclosed copy of val if val contains a delimiter,
	 *         otherwise returns the original val.
	 */
	private String quoteDelimiter(String val) {
		// Check if appended value contains a delimiter
		if (val != null
				&& (val.indexOf(colDlm) != -1 || val.indexOf(rowDlm) != -1)) {
			// Encapsulate value in quotes so delimiter can be ignored
			return "\"" + val + "\"";
		} else {
			return val;
		}
	}

	private BufferedWriter getWriter() {
		File fi = new File(fileName);

		// Create a new file if one does not already exist
		if (!fi.exists()) {
			try {
				fi.createNewFile();
			} catch (IOException e) {
				System.err.println("Failed to create new file with name: "
						+ fileName);
				e.printStackTrace();
				return null;
			}
		}

		// Create a BufferedWriter to send text to the specified file
		FileWriter fw;
		try {
			fw = new FileWriter(fi.getAbsoluteFile());
		} catch (IOException e) {
			System.err.println("Failed to open file writing stream for file: "
					+ fileName);
			e.printStackTrace();
			return null;
		}

		return new BufferedWriter(fw);
	}
}
