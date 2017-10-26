package gr.ilsp.fmc.mysql;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class MySQLConnector {
	//private static String hostname;
	//private static String usr;
	//private static String pas;
	private static Connection con=null;
	
	public static int Connect(String host, String usrname, String password)
	{
		Properties properties = new Properties();
		properties.put("user", usrname);
		properties.put("password", password);
		//properties.put("characterEncoding", "UTF-8");
		//properties.put("characterSetResults", "UTF-8");
		//properties.put("useUnicode", "true");
		//String url = "jdbc:mysql://"+host;
		String url = "jdbc:mysql://"+host+"?characterSetResults=UTF-8&characterEncoding=UTF-8&useUnicode=yes";
		  
		
		//hostname = host;
		//usr = usrname;
		//pas = password;
		con = null;
		//String utfEnc = "?useUnicode=true&amp;characterEncoding=UTF-8";
	    try {
       	  
	
	    	
	      Class.forName("com.mysql.jdbc.Driver").newInstance();
	      con = DriverManager.getConnection(url,properties);
	     // DriverManager.getConnection("jdbc:mysql://");
	      
	      
	      if(!con.isClosed())
	        {
	    	  //System.out.println("Successfully connected to " +
	          //"MySQL server");
	    	  return 1;
	      }
	      else
	    	  return 0;
	    } catch(Exception e) {
	      System.err.println("Exception: " + e.getMessage());
	      return 0;
	    }

	}
	public static void Disconnect()
	{
		try {
	        if(con != null)
	          con.close();
	      } catch(SQLException e) {
	    	  System.err.println(e.getMessage());
	    	  }
	}
	public static ResultSet RunQuery(String query)
	{		
		Statement stmt = null;
		try{
			stmt = con.createStatement();			
			return stmt.executeQuery(query);
		}
		catch(SQLException e)
		{
			System.err.println("Exception:"+e.getMessage());
			return null;
		}			
	}
	public static boolean Run(String query)
	{
		Statement stmt = null;
		try{
			stmt = con.createStatement();
			return stmt.execute(query);
		}
		catch(SQLException e)
		{
			System.err.println("Exception:"+e.getMessage());
			return false;
		} 
	}
}
	
