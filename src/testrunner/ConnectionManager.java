package testrunner;

import java.sql.Connection;

public interface ConnectionManager {

	Connection getTargetConnection();

	Connection getSourceConnection();
	
	boolean verifyDriver();
}
