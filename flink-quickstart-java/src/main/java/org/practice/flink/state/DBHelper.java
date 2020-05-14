/**
 * 
 */
package org.practice.flink.state;

import static org.apache.flink.configuration.ConfigOptions.key;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

/**
 * @author Ramesh
 *
 */
public class DBHelper {

	public static final ConfigOption<String> DATABASE_URL = key("mysql.db.url")
			.defaultValue("jdbc:mysql://localhost/transaction").withDescription("My SQL DB URL");

	private static final ConfigOption<String> DATABASE_USER = key("mysql.db.user")
			.defaultValue("jdbc:mysql://localhost/transaction").withDescription("My SQL DB User");

	private static final ConfigOption<String> DATABASE_PASSWORD = key("mysql.db.password")
			.defaultValue("jdbc:mysql://localhost/transaction").withDescription("My SQL DB Password");

	/**
	 * 
	 * @param configuration
	 * @return
	 */
	public static String getDBUrl(Configuration configuration) {
		return configuration.get(DATABASE_URL);
	}

	/**
	 * 
	 * @param configuration
	 * @return
	 */
	public static Connection getDBConnection(Configuration configuration) {
		Connection connection = null;
		try {

			Class.forName("com.mysql.jdbc.Driver");

			// STEP 3: Open a connection
			System.out.println("Connecting to a selected database...");

			connection = DriverManager.getConnection(configuration.get(DATABASE_URL), configuration.get(DATABASE_URL),
					configuration.get(DATABASE_PASSWORD));

			System.out.println("Connected database successfully...");

		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// finally block used to close resources
			try {
				if (connection != null)
					connection.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}

		return connection;
	}

	/**
	 * 
	 * @param connection
	 * @return
	 */
	public static List<Transaction> fetchTransactions(Connection connection) {

		Statement stmt = null;

		try {
			stmt = connection.createStatement();

			String sql = "SELECT id, date, ecn, amount, status FROM TRANSACTIONS where status = SUBMITTED";
			ResultSet rs = stmt.executeQuery(sql);
			// STEP 5: Extract data from result set
			while (rs.next()) {
				// Retrieve by column name
				String id = rs.getString("id");
				Date age = rs.getDate("date");
				String first = rs.getString("ecn");
				String last = rs.getString("amount");
			}
			rs.close();
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					connection.close();
			} catch (SQLException se) {
			} // do nothing
			try {
				if (connection != null)
					connection.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}

		return null;
	}
}
