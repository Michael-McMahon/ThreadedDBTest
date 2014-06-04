package testrunner;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestRunner {
	// Tracks the number of running tester threads.
	private int running = 0;

	/*
	 * POOL_SIZE sets maximum number of threads create. Using the number of
	 * cores/processors.
	 */
	private final static int POOL_SIZE = Runtime.getRuntime()
			.availableProcessors();
	private final static String FILE_NAME = "RESULTS";
	private final static String FILE_EXT = ".csv";

	static String getResultCount = "SELECT COUNT(*)"
			+ " FROM ORGANIZATION_DOMAINS T";

	/**
	 * Increment the number of running threads. Is private since only this class
	 * will start new threads. Synchronized because other threads can access the
	 * decrementRunning method.
	 */
	private synchronized void incrementRunning() {
		running++;
	}

	/**
	 * Decrement the number of running threads. Synchronized because multiple
	 * threads can call this method.
	 */
	public synchronized void decrementRunning() {
		running--;
		notify();
	}

	/**
	 * Runs up to POOL_SIZE number of TesterThreads, with each Thread given a
	 * share of the total records to test.
	 */
	private void runTest() {
		int i, min, max;
		TesterThread tester;

		// Get the min and max rows to be processed by each TesterThread
		int[] ranges = getRanges();
		if (ranges == null) {
			return;
		}
		if (ranges[0] == -1) {
			System.err.println("No test records found in target table.");
			return;
		}

		// Get formatted date for consistent tagging of the result files
		String date = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

		// First TestThread created with min row of 1
		max = ranges[0];
		tester = new TesterThread(1, max, this, new StageConnectionManager(),
				createFileName(1, max, date));
		incrementRunning();
		tester.run();

		// Create a total of POOL_SIZE TesterThreads
		for (i = 1; i < POOL_SIZE; i++) {
			// ranges[i] will be -1 when result size is less than i
			if (ranges[i] > 0) {
				// Min of next thread is max of last thread plus one
				min = max + 1;
				max = ranges[i];
				tester = new TesterThread(min, max, this,
						new StageConnectionManager(), createFileName(min, max,
								date));
				incrementRunning();
				tester.run();
			}
		}

		// This thread is locked until all TesterThreads complete
		while (running > 0) {
			try {
				wait();
			} catch (InterruptedException e) {
				System.err.println("Process was interrupted. Some records "
						+ "were not tested.");
				e.printStackTrace();
			}
		}
	}

	/**
	 * Create filename with this format: [date]_[FILE_NAME]_[start row]_[end
	 * row].[FILE_EXT]
	 */
	private String createFileName(int min, int max, String date) {
		// Separator for filename fields
		String sep = "_";
		StringBuilder sb = new StringBuilder();

		sb.append(date).append(sep).append(FILE_NAME).append(sep).append(min)
				.append(sep).append(max).append(".").append(FILE_EXT);

		return sb.toString();
	}

	/**
	 * Get the range of rows to be evaluated by each TesterThread.
	 * 
	 * @return An array of ints, where the i'th entry is the maximum row number
	 *         to be processed by the i'th TesterTread. i'th entry set to -1 if
	 *         thread does not need to be run.
	 */
	private int[] getRanges() {
		int max, quo, rem, i;
		int[] ranges = new int[POOL_SIZE];

		max = getResultSize();
		if (max < 0) {
			return null;
		}

		//Output count so user knows how many records will be tested
		System.out.println("Testing " + max + " records.");
		
		// Get remainder after dividing by number of threads
		rem = max % POOL_SIZE;
		// Get quotient after dividing by number of threads
		quo = max / POOL_SIZE;

		// Special case if POOL_SIZE is greater than result size
		if (quo == 0) {
			for (i = 0; i < POOL_SIZE; i++) {
				if (rem-- > 0) {
					// Each thread will process one row
					ranges[i] = i + 1;
				} else {
					// Flag remaining ranges as invalid
					ranges[i] = -1;
				}
			}
		} else {
			max = quo;
			for (i = 0; i < POOL_SIZE; i++) {
				// Check for rows in the remainder
				if (rem > 0) {
					// Add extra row to cover remainder row
					max++;
					rem--;
				}

				ranges[i] = max;
				// Increment max to the end of the next range
				max += quo;
			}
		}

		return ranges;
	}

	private int getResultSize() {
		try {
			Connection con = new StageConnectionManager().getTargetConnection();
			try {
				Statement stmt = con.createStatement();
				try {
					ResultSet res = stmt.executeQuery(getResultCount);
					try {
						if (res.next()) {
							// Get total number of records to be tested
							int size = res.getInt(1);
							return size;
						} else {
							return -1;
						}
					} catch (SQLException e) {
						System.err.println("Failed to fetch result from "
								+ "ResultSet of query: " + getResultCount);
						e.printStackTrace();
						return -1;
					}
				} catch (SQLException e) {
					System.err.println("Failed to execute query: "
							+ getResultCount);
					e.printStackTrace();
					return -1;
				}
			} catch (SQLException e) {
				System.err.println("Failed to create Statement from target "
						+ "database connection");
				e.printStackTrace();
				return -1;
			} finally {
				con.close();
			}
		} catch (SQLException e) {
			System.err.println("Failed to connect to target database");
			e.printStackTrace();
			return -1;
		}
	}

	public static void main(String[] args) {
		System.out.println("Starting at: " + new Date().toString());

		if (!StageConnectionManager.verifyDriver()) {
			System.err.println("JDBC Driver not found. No records were "
					+ "tested");
			return;
		}
		new TestRunner().runTest();

		System.out.println("Ending at: " + new Date().toString());
	}
}
