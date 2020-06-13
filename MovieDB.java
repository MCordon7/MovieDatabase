package jdbc_example;

import java.sql.*;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
users(userID, gender, age, occupation, zipcode)
occ(occNUM, occ)
age(ageNUM, age)
ratingsID(userID, movieID)
ratingsDESC(userID, rating, time)
movieIDS(movieID, movieTITLE)
movieDESC(movieTITLE, year, genre)
*/

public class MovieDB {
	String tableName = null;
	Connection conn = null;
	Statement stmt = null;

	public MovieDB(String connection, String user, String pass) {
		login(connection, user, pass);
	}

	public void setTable(String t) {
		this.tableName = t;
	}

	public String getTable() {
		return tableName;
	}

	public void login(String connection, String user, String pass) {
		/***********************************************************************
		 *  determine if the JDBC driver exists and load it...
		 ***********************************************************************/
		System.out.print( "\nLoading JDBC driver...\n\n" );
		try {
			Class.forName("oracle.jdbc.OracleDriver");
		}
		catch(ClassNotFoundException e) {
			System.out.println(e);
			System.exit(1);
		}

		/***********************************************************************
		 *  establish a connection to the database...
		 ***********************************************************************/
		try {
			System.out.print( "Connecting to DEF database...\n\n" );
			//String url = dataSource + dbName;

			conn = DriverManager.getConnection(connection, user, pass);


			/*conn = dbms.equals("localAccess") ? DriverManager.getConnection(url)
		            : DriverManager.getConnection(url, userName, password );*/
			System.out.println( "Connected to database DEF...\n" );

			/***********************************************************************
			 *  create an object by which we will pass SQL stmts to the database...
			 ***********************************************************************/
			stmt = conn.createStatement();
		}
		catch (SQLException se) {
			System.out.println(se);
			System.exit(1);
		}
	}

	public void logout() {
		try {
			stmt.close();
			conn.close();
			System.out.println("You have successfully logged out.");
		} catch (SQLException e) {
			System.out.println( "SQL ERROR: " + e );
		}
	}


	//Creates and populates the user table.
	public void createUser() {
		tableName = "users";
		try {
			String dropString = "DROP TABLE " + tableName + " CASCADE CONSTRAINTS";
			stmt.executeUpdate(dropString);
		}
		catch (SQLException se) {/*do nothing*/} // table doesn't exist

		try {
			/***********************************************************************
			 *  create the new table...
			 ***********************************************************************/
			System.out.print( "Building new " + tableName + " table...\n\n" );
			String createString =
					"CREATE TABLE " + tableName +
					"  (userID NUMBER," +
					"   gender VARCHAR2(10)," +
					"	age NUMBER," +
					"   occupation VARCHAR2(10)," +
					"   zipcode VARCHAR2(10)," +
					"	CONSTRAINT PK_userIDS" +
					" 	PRIMARY KEY (userID))";
			stmt.executeUpdate(createString);
		} catch(SQLException e) {
			System.out.println( "SQL ERROR: " + e );
		}

		popUser();
	}

	//Creates and populates the occupation table.
	public void createOcc() {
		setTable("occ");
		try {
			String dropString = "DROP TABLE " + tableName + " CASCADE CONSTRAINTS";
			stmt.executeUpdate(dropString);
		}
		catch (SQLException se) {/*do nothing*/} // table doesn't exist

		try {
			/***********************************************************************
			 *  create the new table...
			 ***********************************************************************/
			System.out.print( "Building new " + "Occupation" + " table...\n\n" );
			String createString =
					"CREATE TABLE " + tableName +
					"  (occNUM NUMBER," +
					"   occ VARCHAR2(120)," +
					"   CONSTRAINT PK_occNUM" + 
					"   PRIMARY KEY (occNUM))";
			stmt.executeUpdate(createString);
		} catch(SQLException e) {
			System.out.println( "SQL ERROR: " + e );
		}

		popOcc();
	}

	//Creates and populates the age table.
	public void createAge() {
		setTable("age");
		try {
			String dropString = "DROP TABLE " + tableName + " CASCADE CONSTRAINTS";
			stmt.executeUpdate(dropString);
		}
		catch (SQLException se) {/*do nothing*/} // table doesn't exist

		try {
			/***********************************************************************
			 *  create the new table...
			 ***********************************************************************/
			System.out.print( "Building new " + "Age" + " table...\n\n" );
			String createString =
					"CREATE TABLE " + tableName +
					"  (ageNUM NUMBER," +
					"   age VARCHAR2(10)," +
					"   CONSTRAINT PK_ageNUM" + 
					"   PRIMARY KEY (ageNUM))";
			stmt.executeUpdate(createString);
		} catch(SQLException e) {
			System.out.println( "SQL ERROR: " + e );
		}
		popAge();

	}

	//Creates and populates the RatingsID and RatingsDESC table.
	public void createRID() {
		setTable("ratingsID");
		try {
			String dropString = "DROP TABLE " + getTable() + " CASCADE CONSTRAINTS";
			stmt.executeUpdate(dropString);
		}
		catch (SQLException se) {/*do nothing*/} // table doesn't exist

		try {
			/***********************************************************************
			 *  create the new table...
			 ***********************************************************************/
			System.out.print( "Building new " + "RatingsID" + " table...\n\n" );
			String createString =
					"CREATE TABLE " + tableName +
					"  (userID NUMBER," +
					"   movieID NUMBER," +	
					" 	FOREIGN KEY (movieID) REFERENCES movieIDS(movieID)," +
					"	FOREIGN KEY (userID) REFERENCES users(userID))";
			stmt.executeUpdate(createString);
		} catch(SQLException e) {
			System.out.println( "SQL ERROR: " + e );
		}
		createRDESC();
		popRID();
	}

	//Creates Ratings Description table.
	public void createRDESC() {
		setTable("ratingsDESC");
		try {
			String dropString = "DROP TABLE " + getTable() + " CASCADE CONSTRAINTS";
			stmt.executeUpdate(dropString);
		}
		catch (SQLException se) {/*do nothing*/} // table doesn't exist

		try {
			/***********************************************************************
			 *  create the new table...
			 ***********************************************************************/
			System.out.print( "Building new " + "RatingsDesc" + " table...\n\n" );
			String createString =
					"CREATE TABLE " + tableName +
					"  (userID NUMBER," +
					"   rating NUMBER," +
					"	time NUMBER)";
			stmt.executeUpdate(createString);
		} catch(SQLException e) {
			System.out.println( "SQL ERROR: " + e );
		}
	}

	//Creates and populates Movie tables.
	public void createMT() {
		setTable("movieIDS");
		try {
			String dropString = "DROP TABLE " + getTable() + " CASCADE CONSTRAINTS";
			stmt.executeUpdate(dropString);
		}
		catch (SQLException se) {/*do nothing*/} // table doesn't exist

		try {
			/***********************************************************************
			 *  create the new table...
			 ***********************************************************************/
			System.out.print( "Building new " + getTable() + " table...\n\n" );
			String createString =
					"CREATE TABLE " + tableName +
					"  (movieID NUMBER," +
					"   movieTITLE VARCHAR2(120)," +	
					"	CONSTRAINT PK_movieIDS" +
					" 	PRIMARY KEY (movieID))";
			stmt.executeUpdate(createString);
		} catch(SQLException e) {
			System.out.println( "SQL ERROR: " + e );
		}
		createMD();
		createMG();
		popMT();
	}

	//Creates the Movie Description table.
	public void createMD() {
		setTable("movieDESC");
		try {
			String dropString = "DROP TABLE " + getTable() + " CASCADE CONSTRAINTS";
			stmt.executeUpdate(dropString);
		}
		catch (SQLException se) {/*do nothing*/} // table doesn't exist

		try {
			/***********************************************************************
			 *  create the new table...
			 ***********************************************************************/
			System.out.print( "Building new " + getTable() + " table...\n\n" );
			String createString =
					"CREATE TABLE " + tableName +
					"  (movieTITLE VARCHAR2(120)," +
					"   year NUMBER)";
			stmt.executeUpdate(createString);
		} catch(SQLException e) {
			System.out.println( "SQL ERROR: " + e );
		}

	}

	//Creates the Movie genre table.
	public void  createMG() {
		setTable("movieGENRE");
		try {
			String dropString = "DROP TABLE " + getTable() + " CASCADE CONSTRAINTS";
			stmt.executeUpdate(dropString);
		}
		catch (SQLException se) {/*do nothing*/} // table doesn't exist

		try {
			/***********************************************************************
			 *  create the new table...
			 ***********************************************************************/
			System.out.print( "Building new " + getTable() + " table...\n\n" );
			String createString =
					"CREATE TABLE " + tableName +
					"  (movieTITLE VARCHAR2(120)," +
					"	genre VARCHAR2(120))";
			stmt.executeUpdate(createString);
		} catch(SQLException e) {
			System.out.println( "SQL ERROR: " + e );
		}

	}
	
	public void popMT() {
		String mID = "movieIDS";
		String mDESC = "movieDESC";
		String mG = "movieGENRE";
		try {
			System.out.print( "Inserting rows into Movie tables...\n\n" );
			File file = new File("movies.dat");
			String path = file.getAbsolutePath();
			FileReader fileRead = new FileReader(path);
			BufferedReader buffRead = new BufferedReader(fileRead);
			String sLine;
			String splitBy = "::";
			String splitG = "\\|";
			String splitY = "\\(([0-9]*?)\\)";
			PreparedStatement updateMID =
					conn.prepareStatement( "INSERT INTO " + mID + " VALUES ( ?, ? )" );

			PreparedStatement updateMDESC = 
					conn.prepareStatement( "INSERT INTO " + mDESC + " VALUES ( ?, ? )" );
			
			PreparedStatement updateMG = 
					conn.prepareStatement( "INSERT INTO " + mG + " VALUES ( ?, ? )" );
			conn.setAutoCommit(false);

			while((sLine = buffRead.readLine()) != null) {
				String[] data = sLine.split(splitBy);
				String[] dataG = data[2].split(splitG);

				//Adds the movie ID and movie Titles into the movieIDS table
				updateMID.setString(1, data[0]);
				updateMID.setString(2, data[1].replaceAll(splitY, ""));

				//Adds the title into the movieDESC table
				updateMDESC.setString(1, data[1].replaceAll(splitY, ""));
				
				//Adds the year into the movieDESC table
				Matcher m = Pattern.compile(splitY).matcher(data[1]);
				m.find();
				updateMDESC.setString(2, m.group(1));

				for(int i = 0; i < dataG.length; i++) {
					//Adds the genres into the movieDESC table
					updateMG.setString(1, data[1].replaceAll(splitY, ""));
					updateMG.setString(2, dataG[i]);
					updateMG.addBatch();
				}
				updateMDESC.addBatch();
				updateMID.addBatch();
			}
			
			updateMID.executeBatch();
			updateMDESC.executeBatch();
			updateMG.executeBatch();
			updateMID.clearBatch();
			updateMDESC.clearBatch();
			updateMG.clearBatch();
			fileRead.close();
			buffRead.close();
			conn.commit();
			System.out.println("Rows successfully inserted\n");
		} catch(SQLException | IOException e) {
			System.out.println( "SQL ERROR: " + e );
		}
	}

	public void popRID() {

		String table1 = "ratingsID";
		String table2 = "ratingsDESC";
		try {
			System.out.print( "Inserting rows into Ratings tables...\n\n" );
			File file = new File("ratings.dat");
			String path = file.getAbsolutePath();
			FileReader fileRead = new FileReader(path);
			BufferedReader buffRead = new BufferedReader(fileRead);
			String sLine;
			String splitBy = "::";

			PreparedStatement updateRID =
					conn.prepareStatement( "INSERT INTO " + table1 + " VALUES ( ?, ? )" );

			PreparedStatement updateRDESC =
					conn.prepareStatement( "INSERT INTO " + table2 + " VALUES ( ?, ?, ? )" );

			conn.setAutoCommit(false);
			while((sLine = buffRead.readLine()) != null) {
				String[] data = sLine.split(splitBy);
				updateRID.setString(1, data[0]);
				updateRID.setString(2, data[1]);
				updateRDESC.setString(1, data[0]);
				updateRDESC.setString(2, data[2]);
				updateRDESC.setString(3, data[3]);
				updateRID.addBatch();
				updateRDESC.addBatch();
				/*
				if(insertTotal++ == ()) {
					updateRID.executeBatch();
					updateRID.clearBatch();
					insertTotal = 0;

				}*/
			}
			updateRID.executeBatch();
			updateRID.clearBatch();
			updateRDESC.executeBatch();
			updateRDESC.clearBatch();
			fileRead.close();
			buffRead.close();
			conn.commit();
			System.out.println("Rows successfully inserted\n");
		} catch(SQLException | IOException e) {
			System.out.println( "SQL ERROR: " + e );
		}
	}

	public void popOcc() {
		setTable("occ");
		try {
			System.out.print( "Inserting rows into Occupation table...\n\n" );
			Map<String, String> occMap = new HashMap<String, String>();
			PreparedStatement updateOcc = conn.prepareStatement( "INSERT INTO " + tableName + " VALUES(?, ?)");
			occMap.put("0", "'other' or not specified");
			occMap.put("1", "academic/educator");
			occMap.put("2", "artist");
			occMap.put("3", "clerical/admin");
			occMap.put("4", "college/grad student");
			occMap.put("5", "customer service");
			occMap.put("6", "doctor/health care");
			occMap.put("7", "executive/managerial");
			occMap.put("8", "farmer");
			occMap.put("9", "homemaker");
			occMap.put("10", "K-12 student");
			occMap.put("11", "lawyer");
			occMap.put("12", "programmer");
			occMap.put("13", "retired");
			occMap.put("14", "sales/marketing");
			occMap.put("15", "scientist");
			occMap.put("16", "self-employed");
			occMap.put("17", "technician/engineer");
			occMap.put("18", "tradesman/craftsman");
			occMap.put("19", "unemployed");
			occMap.put("20", "writer");


			conn.setAutoCommit(false);
			for(Map.Entry<String,String> occ : occMap.entrySet()) {
				updateOcc.setString(1, occ.getKey());
				updateOcc.setString(2, occ.getValue());
				updateOcc.executeUpdate();
			}
			conn.commit();
			System.out.println("Rows successfully inserted\n");
		} catch(SQLException e) {
			System.out.println("SQL Error: " + e);
		}
	}

	public void popAge() {
		setTable("age");
		try {
			System.out.print( "Inserting rows into Age table...\n\n" );
			Map<String, String> ageMap = new HashMap<String, String>();
			PreparedStatement updateAge = conn.prepareStatement( "INSERT INTO " + tableName + " VALUES(?, ?)");
			ageMap.put("1", "Under 18");
			ageMap.put("18", "18-24");
			ageMap.put("25", "25-34");
			ageMap.put("35", "35-44");
			ageMap.put("45", "45-49");
			ageMap.put("50", "50-55");
			ageMap.put("56", "56+");

			conn.setAutoCommit(false);
			for(Map.Entry<String,String> age : ageMap.entrySet()) {
				updateAge.setString(1, age.getKey());
				updateAge.setString(2, age.getValue());
				updateAge.executeUpdate();
			}
			conn.commit();
			
			System.out.println("Rows successfully inserted\n");
		} catch(SQLException e) {
			System.out.println("SQL Error: " + e);
		}
	}

	public void popUser() {
		setTable("users");
		try {
			System.out.print( "Inserting rows into Users table...\n\n" );
			File file = new File("users.dat");
			String path = file.getAbsolutePath();
			FileReader fileRead = new FileReader(path);
			BufferedReader buffRead = new BufferedReader(fileRead);
			String sLine;
			String splitBy = "::";

			PreparedStatement updateUsers =
					conn.prepareStatement( "INSERT INTO " + tableName + " VALUES ( ?, ?, ?, ?, ? )" );

			conn.setAutoCommit(false);

			while((sLine = buffRead.readLine()) != null) {
				String[] data = sLine.split(splitBy);
				updateUsers.setString(1, data[0]);
				updateUsers.setString(2, data[1]);
				updateUsers.setString(3, data[2]);
				updateUsers.setString(4, data[3]);
				updateUsers.setString(5, data[4]);
				updateUsers.addBatch();
			}

			updateUsers.executeBatch();
			updateUsers.clearBatch();
			fileRead.close();
			buffRead.close();
			conn.commit();
			System.out.println("Rows successfully inserted\n");
		} catch(SQLException | IOException e) {
			System.out.println( "SQL ERROR: " + e );
		}
	}

	public void query() {
		try {
			int i = 0;
			ResultSet rset = stmt.executeQuery("SELECT COUNT(users.userID)NumberofReviews, occ.occ, movieIDS.movieTITLE, ratingsDESC.rating\r\n" + 
					"    FROM users RIGHT JOIN occ ON users.occupation=occ.occNUM\r\n" + 
					"                    RIGHT JOIN ratingsID ON users.userID=ratingsID.userID\r\n" + 
					"                    RIGHT JOIN ratingsDESC ON ratingsDESC.userID=ratingsID.userID\r\n" + 
					"                    RIGHT JOIN movieIDS ON ratingsID.movieID=movieIDS.movieID\r\n" + 
					"    WHERE occ.occNUM = 2 AND ratingsDESC.rating = 5\r\n" + 
					"    GROUP BY movieIDS.movieTITLE, occ.occ, ratingsDESC.rating\r\n" + 
					"    ORDER BY COUNT(users.userID) DESC");

			while(rset.next()) {
				if(i >= 10) {
					break;
				}
				System.out.println("Number of Reviews: " + rset.getString("NumberofReviews")+ "| Occupation: " + rset.getString("occ") 
									+ "| Movie: " + rset.getString("movieTITLE") + "| Rating: " + rset.getString("rating"));	
				i++;
			}

			rset.close();

		} catch(SQLException e) {
			System.out.println("SQL Error: "+ e);
		}

	}

	public static void main(String[] args) {
		MovieDB test = new MovieDB("jdbc:oracle:thin:@acadoradbprd01.dpu.depaul.edu:1521:ACADPRD0", "MCordon", "cdm1493683");
		test.createMT();
		test.createUser();
		test.createAge();
		test.createOcc();
		test.createRID();
		test.query();
		test.logout();
	}
}

