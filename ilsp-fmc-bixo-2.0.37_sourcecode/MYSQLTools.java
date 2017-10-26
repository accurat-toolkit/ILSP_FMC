package gr.ilsp.fmc.mysql;

import gr.ilsp.fmc.main.SimpleCrawl;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MYSQLTools {

	public static boolean initializeDB(String dbname, String dbhost) throws SQLException {
		MySQLConnector.Connect(dbhost, SimpleCrawl.config.getString("mysql_server.user"),
				SimpleCrawl.config.getString("mysql_server.password"));
		ResultSet res = null;
		
		res = MySQLConnector.RunQuery("SHOW DATABASES LIKE '"+dbname+"'");	
		if (res.first()) {
			MySQLConnector.Disconnect();
			return false;
		}
		else {
			MySQLConnector.Run("CREATE schema " + dbname);
			MySQLConnector.Disconnect();
			return true;
		}
		
	}

}
