/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.lang.Math;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */
public class Amazon {

    // reference to physical database connection.
    private Connection _connection = null;
    private int orderNum = 501; // Order number starts at 501 because there are already 500 orders in the table

    // handling the keyboard inputs through a BufferedReader
    // This variable can be global for convenience.
    static BufferedReader in = new BufferedReader(
            new InputStreamReader(System.in));

    /**
     * Creates a new instance of Amazon store
     *
     * @param hostname the MySQL or PostgreSQL server hostname
     * @param database the name of the database
     * @param username the user name used to login to the database
     * @param password the user login password
     * @throws java.sql.SQLException when failed to make a connection.
     */
    public Amazon(String dbname, String dbport, String user, String passwd) throws SQLException {

        System.out.print("Connecting to database...");
        try {
            // constructs the connection URL
            String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
            System.out.println("Connection URL: " + url + "\n");

            // obtain a physical connection
            this._connection = DriverManager.getConnection(url, user, passwd);
            System.out.println("Done");
        } catch (Exception e) {
            System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
            System.out.println("Make sure you started postgres on this machine");
            System.exit(-1);
        } // end catch
    }// end Amazon
    
    // Method to calculate euclidean distance between two latitude, longitude pairs.
    public double calculateDistance(double lat1, double long1, double lat2, double long2) {
        double t1 = (lat1 - lat2) * (lat1 - lat2);
        double t2 = (long1 - long2) * (long1 - long2);
        return Math.sqrt(t1 + t2);
    }

    /**
     * Method to execute an update SQL statement. Update SQL instructions
     * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
     *
     * @param sql the input SQL string
     * @throws java.sql.SQLException when update failed
     */
    public void executeUpdate(String sql) throws SQLException {
        // creates a statement object
        Statement stmt = this._connection.createStatement();

        // issues the update instruction
        stmt.executeUpdate(sql);

        // close the instruction
        stmt.close();
    }// end executeUpdate

    /**
     * Method to execute an input query SQL instruction (i.e. SELECT). This
     * method issues the query to the DBMS and outputs the results to
     * standard out.
     *
     * @param query the input query string
     * @return the number of rows returned
     * @throws java.sql.SQLException when failed to execute the query
     */
    public int executeQueryAndPrintResult(String query) throws SQLException {
        // creates a statement object
        Statement stmt = this._connection.createStatement();

        // issues the query instruction
        ResultSet rs = stmt.executeQuery(query);

        /*
         ** obtains the metadata object for the returned result set. The metadata
         ** contains row and column info.
         */
        ResultSetMetaData rsmd = rs.getMetaData();
        int numCol = rsmd.getColumnCount();
        int rowCount = 0;

        // iterates through the result set and output them to standard out.
        boolean outputHeader = true;
        while (rs.next()) {
            if (outputHeader) {
                for (int i = 1; i <= numCol; i++) {
                    System.out.print(rsmd.getColumnName(i) + "\t");
                }
                System.out.println();
                outputHeader = false;
            }
            for (int i = 1; i <= numCol; ++i)
                System.out.print(rs.getString(i) + "\t");
            System.out.println();
            ++rowCount;
        } // end while
        stmt.close();
        return rowCount;
    }// end executeQuery

    /**
     * Method to execute an input query SQL instruction (i.e. SELECT). This
     * method issues the query to the DBMS and returns the results as
     * a list of records. Each record in turn is a list of attribute values
     *
     * @param query the input query string
     * @return the query result as a list of records
     * @throws java.sql.SQLException when failed to execute the query
     */
    public List<List<String>> executeQueryAndReturnResult(String query) throws SQLException {
        // creates a statement object
        Statement stmt = this._connection.createStatement();

        // issues the query instruction
        ResultSet rs = stmt.executeQuery(query);

        /*
         ** obtains the metadata object for the returned result set. The metadata
         ** contains row and column info.
         */
        ResultSetMetaData rsmd = rs.getMetaData();
        int numCol = rsmd.getColumnCount();
        int rowCount = 0;

        // iterates through the result set and saves the data returned by the query.
        boolean outputHeader = false;
        List<List<String>> result = new ArrayList<List<String>>();
        while (rs.next()) {
            List<String> record = new ArrayList<String>();
            for (int i = 1; i <= numCol; ++i)
                record.add(rs.getString(i));
            result.add(record);
        } // end while
        stmt.close();
        return result;
    }// end executeQueryAndReturnResult

    /**
     * Method to execute an input query SQL instruction (i.e. SELECT). This
     * method issues the query to the DBMS and returns the number of results
     *
     * @param query the input query string
     * @return the number of rows returned
     * @throws java.sql.SQLException when failed to execute the query
     */
    public int executeQuery(String query) throws SQLException {
        // creates a statement object
        Statement stmt = this._connection.createStatement();

        // issues the query instruction
        ResultSet rs = stmt.executeQuery(query);

        int rowCount = 0;

        // iterates through the result set and count nuber of results.
        while (rs.next()) {
            rowCount++;
        } // end while
        stmt.close();
        return rowCount;
    }

    /**
     * Method to fetch the last value from sequence. This
     * method issues the query to the DBMS and returns the current
     * value of sequence used for autogenerated keys
     *
     * @param sequence name of the DB sequence
     * @return current value of a sequence
     * @throws java.sql.SQLException when failed to execute the query
     */
    public int getCurrSeqVal(String sequence) throws SQLException {
        Statement stmt = this._connection.createStatement();

        ResultSet rs = stmt.executeQuery(String.format("Select currval('%s')", sequence));
        if (rs.next())
            return rs.getInt(1);
        return -1;
    }

    /**
     * Method to close the physical connection if it is open.
     */
    public void cleanup() {
        try {
            if (this._connection != null) {
                this._connection.close();
            } // end if
        } catch (SQLException e) {
            // ignored.
        } // end try
    }// end cleanup

    public int getOrderNum() {
        int num = this.orderNum;
        this.orderNum++;
        
        return num;
    }

    /**
     * The main execution method
     *
     * @param args the command line arguments this inclues the <mysql|pgsql> <login
     *             file>
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println(
                    "Usage: " +
                            "java [-classpath <classpath>] " +
                            Amazon.class.getName() +
                            " <dbname> <port> <user>");
            return;
        } // end if

        Greeting();
        Amazon esql = null;
        try {
            // use postgres JDBC driver.
            Class.forName("org.postgresql.Driver").newInstance();
            // instantiate the Amazon object and creates a physical
            // connection.
            String dbname = args[0];
            String dbport = args[1];
            String user = args[2];
            esql = new Amazon(dbname, dbport, user, "");

            boolean keepon = true;
            while (keepon) {
                // These are sample SQL statements
                System.out.println("MAIN MENU");
                System.out.println("---------");
                System.out.println("1. Create user");
                System.out.println("2. Log in");
                System.out.println("9. < EXIT");
                List<String> userInfo = null;
                switch (readChoice()) {
                    case 1:
                        CreateUser(esql);
                        break;
                    case 2:
                        userInfo = LogIn(esql);
                        break;
                    case 9:
                        keepon = false;
                        break;
                    default:
                        System.out.println("Unrecognized choice!");
                        break;
                }// end switch
                if (userInfo.size() != 0) {
                    boolean usermenu = true;
                    while (usermenu) {
                        System.out.println("MAIN MENU");
                        System.out.println("---------");
                        System.out.println("1. View Stores within 30 miles");
                        System.out.println("2. View Product List");
                        System.out.println("3. Place a Order");
                        System.out.println("4. View 5 recent orders");

                        // the following functionalities basically used by managers
                        System.out.println("5. Update Product");
                        System.out.println("6. View 5 recent Product Updates Info");
                        System.out.println("7. View 5 Popular Items");
                        System.out.println("8. View 5 Popular Customers");
                        System.out.println("9. Place Product Supply Request to Warehouse");

                        System.out.println(".........................");
                        System.out.println("20. Log out");
                        switch (readChoice()) {
                            case 1:
                                viewStores(esql, userInfo.get(0));
                                break;
                            case 2:
                                viewProducts(esql);
                                break;
                            case 3:
                                placeOrder(esql, userInfo.get(0));
                                break;
                            case 4:
                                viewRecentOrders(esql, userInfo.get(0));
                                break;
                            case 5:
                                updateProduct(esql);
                                break;
                            case 6:
                                viewRecentUpdates(esql);
                                break;
                            case 7:
                                viewPopularProducts(esql);
                                break;
                            case 8:
                                viewPopularCustomers(esql, userInfo.get(0), userInfo.get(1));
                                break;
                            case 9:
                                placeProductSupplyRequests(esql);
                                break;

                            case 20:
                                usermenu = false;
                                break;
                            default:
                                System.out.println("Unrecognized choice!");
                                break;
                        }
                    }
                }
            } // end while
        } catch (Exception e) {
            System.err.println(e.getMessage());
        } finally {
            // make sure to cleanup the created table and close the connection.
            try {
                if (esql != null) {
                    System.out.print("Disconnecting from database...");
                    esql.cleanup();
                    System.out.println("Done\n\nBye !");
                } // end if
            } catch (Exception e) {
                // ignored.
            } // end try
        } // end try
    }// end main

    public static void Greeting() {
        System.out.println(
                "\n\n*******************************************************\n" +
                        "              User Interface      	               \n" +
                        "*******************************************************\n");
    }// end Greeting

    /*
     * Reads the users choice given from the keyboard
     * 
     * @int
     **/
    public static int readChoice() {
        int input;
        // returns only if a correct value is given.
        do {
            System.out.print("Please make your choice: ");
            try { // read the integer, parse it and break.
                input = Integer.parseInt(in.readLine());
                break;
            } catch (Exception e) {
                System.out.println("Your input is invalid!");
                continue;
            } // end try
        } while (true);
        return input;
    }// end readChoice

    /*
     * Creates a new user
     **/
    public static void CreateUser(Amazon esql) {
        try {
            System.out.print("\tEnter name: ");
            String name = in.readLine();
            System.out.print("\tEnter password: ");
            String password = in.readLine();
            System.out.print("\tEnter latitude: ");
            String latitude = in.readLine(); // enter lat value between [0.0, 100.0]
            System.out.print("\tEnter longitude: "); // enter long value between [0.0, 100.0]
            String longitude = in.readLine();

            String type = "Customer";

            String query = String.format(
                    "INSERT INTO USERS (name, password, latitude, longitude, type) VALUES ('%s','%s', %s, %s,'%s')",
                    name, password, latitude, longitude, type);

            esql.executeUpdate(query);
            System.out.println("User successfully created!");
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }// end CreateUser

    /*
     * Check log in credentials for an existing user
     * 
     * @return User ID and Type or null is the user does not exist
     **/
    public static List<String> LogIn(Amazon esql) {
        try {
            System.out.print("\tEnter name: ");
            String name = in.readLine();
            System.out.print("\tEnter password: ");
            String password = in.readLine();

            String query = String.format("SELECT userID, type FROM USERS WHERE name = '%s' AND password = '%s'", name, password);
            List<List<String>> result = esql.executeQueryAndReturnResult(query);
            
            List<String> info = new ArrayList<>();
            info.add(result.get(0).get(0));
            info.add(result.get(0).get(1));

            if (info.size() > 0)
                return info;

            return null;
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return null;
        }
    }// end

    // Rest of the functions definition go in here

    public static void viewStores(Amazon esql, String userID) {
        try {
            String query = String.format("SELECT latitude, longitude FROM Users WHERE userID = '%s'", userID);
            
            List<List<String>> results = esql.executeQueryAndReturnResult(query);

            double userLat = Double.parseDouble(results.get(0).get(0));
            double userLong = Double.parseDouble(results.get(0).get(1));

            String query2 = "SELECT storeID, latitude, longitude FROM Store";
            List<List<String>> results2 = esql.executeQueryAndReturnResult(query2);

            System.out.println("List of stores within 30 miles of you");
            System.out.println("---------");
            for (List<String> record : results2) {
                String storeID = record.get(0);
                double storeLat = Double.parseDouble(record.get(1));
                double storeLong = Double.parseDouble(record.get(2));
                double distance = esql.calculateDistance(storeLat, storeLong, userLat, userLong);
                if (distance < 30) {
                    System.out.println("Store ID: " + storeID);
                    System.out.println("Distance: " + distance + " miles");
                    System.out.println("---------");
                    
                }
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public static void viewProducts(Amazon esql) {
        System.out.print("Enter store ID: ");
        
        int id;

        try {
            id = Integer.parseInt(in.readLine());
        } catch (Exception e) {
            System.out.println("Your input is invalid!");
            return;
        }

        try {
            String query = String.format("SELECT productName, numberOfUnits, pricePerUnit FROM Product WHERE storeID = '%s'", id);

            List<List<String>> results = esql.executeQueryAndReturnResult(query);
            
            System.out.println("List of items in Store " + id);
            System.out.println("---------");
            for (List<String> record : results) {
                String name = record.get(0);
                int num = Integer.parseInt(record.get(1));
                double price = Float.parseFloat(record.get(2));
                System.out.println("Item: " + name);
                System.out.println("Units available: " + num);
                System.out.println("Price: " + price);
                System.out.println("---------");
                    
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public static void placeOrder(Amazon esql, String userID) {
        try {
            String query = String.format("SELECT latitude, longitude FROM Users WHERE userID = '%s'", userID);
                
            List<List<String>> results = esql.executeQueryAndReturnResult(query);

            double userLat = Double.parseDouble(results.get(0).get(0));
            double userLong = Double.parseDouble(results.get(0).get(1));

            System.out.print("Enter Store ID: ");
            int storeID = Integer.parseInt(in.readLine());

            String query2 = String.format("SELECT latitude, longitude FROM Store WHERE storeID = '%s'", storeID);
            List<List<String>> results2 = esql.executeQueryAndReturnResult(query2);

            if (results2.size() == 0) {
                System.out.println("Store " + storeID + " not found.");
                return;
            }

            double storeLat = Double.parseDouble(results2.get(0).get(0));
            double storeLong = Double.parseDouble(results2.get(0).get(1));
            double distance = esql.calculateDistance(storeLat, storeLong, userLat, userLong);
            
            if (distance > 30) {
                System.out.println("Store " + storeID + " too far from current location.");
                return;
            }
            
            System.out.print("\nEnter product name: ");
            String productName = in.readLine();

            String query3 = String.format("SELECT numberOfUnits FROM Product WHERE storeID = '%s' AND productName = '%s'", storeID, productName);
            List<List<String>> results3 = esql.executeQueryAndReturnResult(query3);

            if (results3.size() == 0) {
                System.out.println("Product " + productName + " not found at Store " + storeID + '.');
                return;
            }

            int available = Integer.parseInt(results3.get(0).get(0));

            if (available == 0) {
                System.out.println("Product " + productName + " out of stock at Store " + storeID + '.');
                return;
            }

            System.out.print("\n" + available + " units available. Enter amount of units to purchase: ");
            
            int amount = Integer.parseInt(in.readLine());

            if (amount > available) {
                System.out.println("Not enough units available.");
                return;
            }

            Timestamp time = new Timestamp(new java.util.Date().getTime());

            String query4 = String.format(
                "INSERT INTO Orders (orderNumber, customerID, storeID, productName, unitsOrdered, orderTime) VALUES ('%s', '%s', '%s', '%s', '%s', '%s')",
                esql.getOrderNum(), userID, storeID, productName, amount, time);

            esql.executeUpdate(query4);
            System.out.println("Order placed!");
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public static void viewRecentOrders(Amazon esql, String userID) {
        try {
            String query = String.format("SELECT storeID, productName, unitsOrdered, orderTime FROM Orders WHERE customerID = '%s' ORDER BY orderTime DESC LIMIT 5", userID);
            
            List<List<String>> results = esql.executeQueryAndReturnResult(query);
            
            System.out.println("\nRecent Orders");
            System.out.println("---------");
            for (List<String> record : results) {
                int storeID = Integer.parseInt(record.get(0));
                String productName = record.get(1);
                int unitsOrdered = Integer.parseInt(record.get(2));
                String date = record.get(3);

                System.out.println("Store ID: " + storeID);
                System.out.println("Product name: " + productName);
                System.out.println("Units ordered: " + unitsOrdered);
                System.out.println("Date ordered: " + date);

                System.out.println("---------");    
            }

            System.out.println();

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public static void updateProduct(Amazon esql) {
    }

    public static void viewRecentUpdates(Amazon esql) {
    }

    public static void viewPopularProducts(Amazon esql) {
    }

    public static void viewPopularCustomers(Amazon esql, String userID, String type) {
        if (!type.trim().equals("manager")) {
            System.out.println("Invalid permissions.\n");
            return;
        }

        try {
            String query = String.format("SELECT storeID from Store WHERE managerID = '%s'", userID);
            List<List<String>> result = esql.executeQueryAndReturnResult(query);

            if (result.size() == 0) {
                System.out.println("You do not manage any stores.");
                return;
            }

            System.out.println("Here are the stores you manage: ");
            System.out.println("---------");  
            for (List<String> record : result) {
                System.out.println("Store ID: " + record.get(0));
            }

            System.out.println("---------\n");  
            
            System.out.print("Enter store ID to view popular customers: ");
            String storeID = in.readLine();

            String query2 = String.format("SELECT u.userID, u.name, COUNT(o.orderNumber) AS orderCount FROM Users u JOIN Orders o ON u.userID = o.customerID WHERE o.storeID = '%s' GROUP BY u.userID, u.name ORDER BY orderCount DESC LIMIT 5", storeID);
            List<List<String>> result2 = esql.executeQueryAndReturnResult(query2);

            System.out.println("\nMost popular customers at Store " + storeID);
            System.out.println("---------");
            for (List<String> record : result2) {
                String customerID = record.get(0);
                String name = record.get(1);
                String orderCount = record.get(2);

                System.out.println("Customer ID: " + customerID);
                System.out.println("Name: " + name);
                System.out.println("Order count: " + orderCount);

                System.out.println("---------");    
            }

            System.out.println();

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

    }

    public static void placeProductSupplyRequests(Amazon esql) {
    }

}// end Amazon
