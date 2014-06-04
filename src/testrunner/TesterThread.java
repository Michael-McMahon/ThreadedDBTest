package testrunner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

class TesterThread implements Runnable {
	private int min, max;
	private TestRunner runner;
	private String fileName;
	private ConnectionManager conMan;

	//Source table table query to get email domains associated to an organization
	private String getExpectedDomains = "SELECT DOMAIN "
			+ " FROM"
			+ " (SELECT DISTINCT SUBSTR(CONTACT.EMAIL_ADDR, 1+INSTR(CONTACT.EMAIL_ADDR, '@')) DOMAIN"
			+ " FROM" + " CONTACT_TABLE CONTACT"
			+ " INNER JOIN CONTACT_ORGANIZATION CO"
			+ " ON CONTACT.CONTACT_ID = CO.CONTACT_ID"
			+ " INNER JOIN ORGANIZATION ORG"
			+ " ON ORG.ORGANIZATION_ID = CO.ORGANIZATION_ID" + " WHERE" + " ORG.KEY = ? )"
			+ " WHERE "
			// +" --Domain does not appear after first entry"
			+ " ? NOT LIKE '%,'||DOMAIN||',%'"
			// +" --Domain is not first entry"
			+ " AND ? NOT LIKE DOMAIN||',%'" + " AND ? <> DOMAIN";

	//Target table query to get email domains populated for an organization
	private String getPagedResults = "SELECT KEY, DOMAINS"
			+ " FROM"
			+ " (SELECT KEY, DOMAINS, ROW_NUMBER() OVER (ORDER BY KEY) AS ROWNUM"
			+ " FROM ORGANIZATION_DOMAINS"
			+ " )Q" + " WHERE ROWNUM BETWEEN ? AND ?";

	public TesterThread(int min, int max, TestRunner runner,
			ConnectionManager conMan, String fileName) {
		this.min = min;
		this.max = max;
		this.runner = runner;
		this.conMan = conMan;
		this.fileName = fileName;
	}

	@Override
	public void run() {
		Connection tarCon, srcCon;
		PreparedStatement getMissing, getActual;
		ResultSet missingValues = null;
		ResultSet actualValues;
		String key, actualValue;
		ResultsWriter writer;

		// Create ResultsWriter instance to save results to a file
		writer = new ResultsWriter(fileName);
		// Write column headers to results file. Stop execution if write fails
		if (!writer.writeResults("KEY", "ACTUAL VALUE", "EXPECTED VALUE")) {
			// Release lock which main thread is waiting on
			runner.decrementRunning();
			return;
		}

		// Get connection to target table
		tarCon = conMan.getTargetConnection();
		// Stop execution if Connection fails
		if (tarCon == null) {
			// Release lock which main thread is waiting on
			runner.decrementRunning();
			return;
		}

		// Create parameterized query to get actual values from target
		getActual = createActualValuesStatement(tarCon);
		// Stop execution if PreparedStatement fails
		if (getActual == null) {
			// Release lock which main thread is waiting on
			runner.decrementRunning();
			return;
		}

		// Get actual values to test
		actualValues = getActualValues(getActual);
		// Stop execution if query fails
		if (actualValues == null) {
			// Release lock which main thread is waiting on
			runner.decrementRunning();
			return;
		}

		// Get connection to source table
		srcCon = conMan.getSourceConnection();
		// Stop execution if Connection fails
		if (srcCon == null) {
			// Release lock which main thread is waiting on
			runner.decrementRunning();
			return;
		}

		// Get prepared statement to test for missing values
		getMissing = createMissingValuesStatement(srcCon);
		// Stop execution if PreparedStatement fails
		if (getMissing == null) {
			// Release lock which main thread is waiting on
			runner.decrementRunning();
			return;
		}

		try {
			// Cycle through each key value pair in actual values set
			while (actualValues.next()) {
				// Get next key value pair to test
				key = actualValues.getString(1);
				/*
				 * Note that actualValue is in fact a comma delimited sequence
				 * of values.
				 */
				actualValue = actualValues.getString(2);

				// Use key to lookup expected values missing from actual values
				missingValues = getMissingValues(getMissing, key, actualValue);
				// Stop execution if query fails
				if (missingValues == null) {
					return;
				}
				// Write results to file. Stop execution if write fails
				if (!writeMissingValues(writer, missingValues, key, actualValue)) {
					return;
				}
				/*
				 * Close ResultSet before reference is reassigned. This
				 * statement can throw a SQLException.
				 */
				missingValues.close();
			}
		} catch (SQLException e) {
			System.err.println("Failed to fetch next row from actual values.");
			e.printStackTrace();
			return;
		} finally {
			try {
				srcCon.close();
			} catch (SQLException e) {
				System.err.println("Failed to close Connection to source"
						+ "database.");
				e.printStackTrace();
			}
			try {
				tarCon.close();
			} catch (SQLException e) {
				System.err.println("Failed to close Connection to target"
						+ "database");
				e.printStackTrace();
			}

			// Release lock which main thread is waiting on
			runner.decrementRunning();
		}
	}

	private PreparedStatement createActualValuesStatement(Connection tarCon) {
		try {
			// Create parameterized query to get actual values from target
			PreparedStatement getActual = tarCon
					.prepareStatement(getPagedResults);
			// Set min and max row numbers
			getActual.setInt(1, min);
			getActual.setInt(2, max);
			return getActual;
		} catch (SQLException e) {
			System.err.println("Failed to create PreparedStatement from query:"
					+ getPagedResults);
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Queries the target table to obtain a ResultSet of values to test.
	 * 
	 * @return ResultSet of keyed values to test.
	 */
	private ResultSet getActualValues(PreparedStatement getActual) {
		try {
			// Query for actual values
			ResultSet actualValues = getActual.executeQuery();
			return actualValues;
		} catch (SQLException e) {
			System.err.println("Failed to query actual values from "
					+ "target table.");
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Create a PreparedStatement which compares expected values against actual
	 * values.
	 * 
	 * @return PreparedStatement which compares expected values against actual
	 *         values.
	 */
	private PreparedStatement createMissingValuesStatement(Connection srcCon) {
		try {
			// Create parameterized query to get expected values from source
			PreparedStatement getMissing = srcCon
					.prepareStatement(getExpectedDomains);
			return getMissing;
		} catch (SQLException e1) {
			System.err.println("Failed to create prepared statement from query"
					+ ": " + getExpectedDomains);
			e1.printStackTrace();
			return null;
		}
	}

	/**
	 * Queries the source table to compare expected values against the supplied
	 * actual values.
	 * 
	 * @param getMissing
	 *            PreparedStatement which looks up expected values for the key
	 *            and compares to actual values.
	 * @param key
	 *            Key value associated to the actualValue.
	 * @param actualValue
	 *            The actual value which will be tested.
	 * @return ResultSet of expected values missing from actual value
	 */
	private ResultSet getMissingValues(PreparedStatement getMissing,
			String key, String actualValue) {
		try {
			// Set the primary key in test query
			getMissing.setString(1, key);
			// Set the actual value in test query
			getMissing.setString(2, actualValue);
			getMissing.setString(3, actualValue);
			getMissing.setString(4, actualValue);
		} catch (SQLException e1) {
			System.err.println("Failed to set paramters on "
					+ "PreparedStatement: " + getExpectedDomains
					+ "\nFirst parameter: " + key + "\nSecond Parameter: "
					+ actualValue + "\nThird Parameter: " + actualValue
					+ "\nFourth Parameter: " + actualValue);
			e1.printStackTrace();
			return null;
		}

		try {
			// Query to get difference of expected values minus actual
			ResultSet missingValues = getMissing.executeQuery();
			return missingValues;
		} catch (SQLException e) {
			System.err.println("Failed to query expected values from source "
					+ "table");
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Write missing values to a results file.
	 * 
	 * @param writer
	 *            ResultsWriter for the target results file.
	 * @param missingValues
	 *            ResultSet of missing values.
	 * @param key
	 *            Key associated to the missing values
	 * @param actualValue
	 *            Actual values which are missing expected values
	 * @return True if writing succeeded, otherwise false.
	 */
	private boolean writeMissingValues(ResultsWriter writer,
			ResultSet missingValues, String key, String actualValue) {
		try {
			// Cycle through each missing value
			while (missingValues.next()) {
				// Write entry in results file
				if (!writer.writeResults(key, actualValue, missingValues
						.getString(1))) {
					return false;
				}
			}
		} catch (SQLException e) {
			System.err.println("Failed to fetch next row from missing "
					+ "values.");
			e.printStackTrace();
			return false;
		}

		return true;
	}
}