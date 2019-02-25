
import java.io.BufferedInputStream;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
//import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
//import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
//import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
//import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
//import java.util.UUID;
import java.util.zip.GZIPInputStream;
//import java.util.zip.GZIPOutputStream;
//import java.util.zip.ZipEntry;
//import java.util.zip.ZipInputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
//import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
//import com.amazonaws.regions.Region;
//import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
//import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
//import com.amazonaws.services.s3.model.ListObjectsRequest;
//import com.amazonaws.services.s3.model.ObjectListing;
//import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
//import com.amazonaws.services.s3.model.S3ObjectInputStream;
//import com.amazonaws.services.s3.model.S3ObjectSummary;
//import com.amazonaws.util.IOUtils;


/**
 * This sample demonstrates how to process CSV files from local and from Amazon S3 using the
 * AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon S3. For more information on Amazon
 * S3, see http://aws.amazon.com/s3.
 * <p>
 * Fill in your AWS access credentials in the provided credentials file
 * template, and be sure to move the file to the default location
 * (/Users/yourname/.aws/credentials) where the sample code will load the credentials from.
 * <p>
 * <b>WARNING:</b> To avoid accidental leakage of your credentials, DO NOT keep
 * the credentials file in your source directory.
 *
 * Need to use two parameters. 
 * 1. 'local' or 'aws' 
 * 2. For 'local' use your path to a CSV file. For 'aws' use just bucket plus prefix with a file name.
 * Supported import from local - CSV. From aws CSV and CSV Gziped.
 * 
 */
public class SQLtoSnowflake {
	
	public static boolean  bVerbose = true;//suppress login output/ from Config
	public static String columnDefName = "";
	public static int iHowManyLines2check = 2;
	public static boolean bProcessHeaderFirstLine = true;
	
	private static void outputLogin(boolean bVerbose, String extension) {
		if(bVerbose) System.out.println( extension);
	}
	
	private static void outputLogin(String extension) {
		if(bVerbose) System.out.println("log::" + extension);
	}
	

    public static void main(String[] args) throws IOException {
    	
    	
    	
    	HashMap<String, String> connParams = getProperties();
		HashMap<String, String> paramsInCSV = new HashMap<String, String>();
		String fileNameWithPath = "";
		boolean bIsFileOnCloud = false;
		//https://s3.amazonaws.com/my-first-s3-bucket-ika/star0000-1.csv
		if(args.length == 0)
		{
			System.out.println("Only CSV for now. Path to a CSV file.");
			System.out.println("Need to use two parameters.\n");
			System.out.println("1. 'local' or 'aws'");
			System.out.println("2. For 'local' use your path to a CSV file. For 'aws' use just bucket plus prefix with a file name.");
			System.out.println("Supported import from local - CSV. From aws CSV and CSV Gziped.");
			return;
		}
		else if(args[0].length() > 0)
		{
			if(args[0].contains("aws"))
			{
				bIsFileOnCloud =  true;
			}
			if(args[1].length() > 0)
				fileNameWithPath = args[1];
			else
			{
				System.out.println("Path to file is empty.");
				return;
			}
		}
		
		if(bIsFileOnCloud)
		{
			paramsInCSV = getFileFromS3(fileNameWithPath, connParams);
			try {
				processData2Snow(connParams, paramsInCSV, bIsFileOnCloud);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else
		{
			 String extension = getExtension(fileNameWithPath);
				outputLogin(bVerbose, extension);
				
				if(extension.equalsIgnoreCase("csv"))
				{
					paramsInCSV = processCSV(paramsInCSV, fileNameWithPath, connParams);
				}
				else
				{
					System.out.println("Not CSV extension;");
					return;
				}
				try {
					processData2Snow(connParams, paramsInCSV, bIsFileOnCloud);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		System.out.println("No errors in the app. Please check Snowflake for imported data or errors in a history.");
    }
    
    private static void processData2Snow(HashMap<String, String> connParams, HashMap<String, String> paramsInCSV, boolean bIsFileOnCloud)
			throws SQLException {
		System.out.println("Create JDBC connection");
		Connection connection = getConnection(connParams);
		System.out.println("Done creating JDBC connection\n");
		// create statement
		System.out.println("Create JDBC statement");
		Statement statement = connection.createStatement();
		System.out.println("Done creating JDBC statement\n");
		// create a table
		System.out.println("Create table");
		System.out.println("Table::" + paramsInCSV.get("createTableSql"));
		statement.execute(paramsInCSV.get("createTableSql"));
		statement.close();
		System.out.println("Done creating table\n");
        
        if(bIsFileOnCloud)
        {
        	awspData2Table(connParams, paramsInCSV, statement);
        }
		 else { 
			 csvFile2Table(paramsInCSV, statement); 
			 }
	}

	private static void csvFile2Table(HashMap<String, String> paramsInCSV, Statement statement) throws SQLException {
		System.out.println("Put file into Stage");
		statement.execute("put file:////"+paramsInCSV.get("fileNameWithPath")+" @~/staged");
		statement.close();
		System.out.println("Done put file\n");
		//
		System.out.println("Copy into Table");//DATE_FORMAT
		String sDateFormatSql = "";
		System.out.println("paramsInCSV::" + paramsInCSV.get("dateFormat"));
		if(paramsInCSV.get("dateFormat").length() > 0)
		{
			sDateFormatSql = " DATE_FORMAT = '" + paramsInCSV.get("dateFormat") + "'";
			outputLogin(bVerbose,"Added data format to COPY::" + sDateFormatSql + "\n");
		}
		statement.execute("Copy into "+paramsInCSV.get("fileName")+" from @~/staged file_format = (type = csv field_delimiter = '"+paramsInCSV.get("delimiter")+"' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY='\"' NULL_IF = ('','NULL', 'null', '\\N')  EMPTY_FIELD_AS_NULL = true "+sDateFormatSql+");");
		
		statement.close();
		System.out.println("Done Copy into\n");
		
		System.out.println("Clear stage");
		statement.execute("remove @~/staged pattern='.*.csv.gz';");
		statement.close();
		System.out.println("Done Clear stage\n");
	}

	private static void awspData2Table(HashMap<String, String> connParams, HashMap<String, String> paramsInCSV,
			Statement statement) throws SQLException {
		System.out.println("New stage for s3");
		statement.execute("create or replace stage my_csv_stage url = 's3://"+paramsInCSV.get("bucketName")+"' credentials = (aws_key_id='"+connParams.get("accessKey")+"' aws_secret_key='"+connParams.get("secretKey")+"');");
		statement.close();
		System.out.println("Done New stage for s3\n");
		//
		String sDateFormatSql = "";
		System.out.println("paramsInCSV::" + paramsInCSV.get("dateFormat"));
		if(paramsInCSV.get("dateFormat").length() > 0)
		{
			sDateFormatSql = " DATE_FORMAT = '" + paramsInCSV.get("dateFormat") + "'";
			outputLogin(bVerbose,"Added data format to COPY::" + sDateFormatSql + "\n");
		}
		System.out.println("Copy from s3");
		String sLine2CopyS3 = "copy into "+paramsInCSV.get("fileName")+" from @my_csv_stage/"+paramsInCSV.get("key")+" on_error = 'skip_file' file_format = (type = csv field_delimiter = ',' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY='\"' NULL_IF = ('','NULL', 'null', '\\N')  EMPTY_FIELD_AS_NULL = true "+sDateFormatSql+");\n";
		statement.execute(sLine2CopyS3);
		statement.close();
		//
		System.out.println("Done Copy from s3 to Table:"+paramsInCSV.get("fileName")+" from Stage: @my_csv_stage\n");
		//
		System.out.println("Drop stage: @my_csv_stage");
		statement.execute("DROP STAGE IF EXISTS my_csv_stageurl");
		statement.close();
		System.out.println("Done Drop stage\n");
	}
    
    private static HashMap<String, String> processCSV(HashMap<String, String> paramsInCSV, String fileNameWithPath, HashMap<String, String> propertyParams) throws FileNotFoundException, IOException
    {
    	File fileObject = null;
    	return processCSV(paramsInCSV, fileObject, fileNameWithPath, propertyParams);
    }
    
    private static HashMap<String, String> processCSV(HashMap<String, String> paramsInCSV, File fileObject, String fileNameWithPath, HashMap<String, String> propertyParams) throws FileNotFoundException, IOException {

		paramsInCSV.put("fileNameWithPath", fileNameWithPath);
		String fileName = stripExtension(fileNameWithPath);
		fileName = replaceDotAndUnders(fileName);
		paramsInCSV.put("fileName", fileName);

		outputLogin("fileNameWithPath::" + fileNameWithPath);
		outputLogin("fileName::" + fileName);
		FileReader file2process = null;//
		
		if(fileObject != null)
    	{
			file2process = new FileReader(fileObject);
    	}
		else
		{
			file2process = new FileReader(fileNameWithPath);
		}
		BufferedReader CSVFile = new BufferedReader(file2process);
		
		boolean boolProcessHeaderFirstLine = bProcessHeaderFirstLine;//
		int iCounterLines = 0;//
		//int iHowManyLines2check = 10;//No less then 2 - TWO
		//List<String> listColumns = new ArrayList<String>(); 
		List<String> listTypes = new ArrayList<String>(); 
		LinkedHashMap<String, String> columnsAndDataTypes = new LinkedHashMap<String, String>();
		StringBuilder strColumn = new StringBuilder();
		strColumn.append("CREATE OR REPLACE TABLE "+ fileName +" \n(\n");// table name as file name
		String delimiter = ",";//default delimiter
		String dataRow = CSVFile.readLine();
		String sDateFormat = "";

		while (dataRow != null && iCounterLines < iHowManyLines2check)
		{
			boolean isDateValdated = false;
			if(iCounterLines == 0)
			{
				if (boolProcessHeaderFirstLine) 
					delimiter = fromFirstLineGetColumns(columnsAndDataTypes, dataRow, propertyParams);//Header naming Columns
				else
				{
					delimiter = fromFirstLineGetColumnsNoHeader(dataRow, columnsAndDataTypes, delimiter);
					//Auto naming Columns
				}
			}
			else 
			{
				String[] dataArray = dataRow.split(delimiter);
				String localsDateFormat = "";
				localsDateFormat = evaluateDataTypesFromColumns( columnsAndDataTypes, listTypes, isDateValdated, dataArray, propertyParams);
				if((!localsDateFormat.equals("")))
				{
					if((!localsDateFormat.equals("empty")))
						sDateFormat=localsDateFormat;
				}
			}
			iCounterLines++;
			dataRow = CSVFile.readLine();
		}
		
		
		columnsAndDataTypes.forEach((key,value) -> {
			strColumn.append(key + " " + value  + ","+ "\n");
			outputLogin(key+" : "+value);
		});
		
		strColumn.setLength(strColumn.length() - 2);
		strColumn.append("\n" + ")");
		outputLogin(true,strColumn.toString());
		CSVFile.close();
		paramsInCSV.put("delimiter", delimiter);
		paramsInCSV.put("createTableSql", strColumn.toString());
		paramsInCSV.put("dateFormat", sDateFormat);
		outputLogin("processCSV::sDateFormat::"+sDateFormat);
		return paramsInCSV;//strColumn.toString();
	}

	private static String replaceDotAndUnders(String fileName) {
		if(fileName.contains("-"))
			fileName = fileName.replaceAll("-", "_");
		if(fileName.contains("."))
			fileName = fileName.replaceAll("\\.", "_");
		return fileName;
	}
    
    private static String fromFirstLineGetColumnsNoHeader(String dataRow, LinkedHashMap<String, String> columnsAndDataTypes, String delimiter) {
		if(dataRow.contains("|"))
		{delimiter = "|";}
		else if(dataRow.contains("\t"))
		{delimiter = "\t";}

		String[] dataArray = dataRow.split(delimiter);
		for(int i = 0; i < dataArray.length; i++)
		{
			//listColumns.add(columnDefName + i);
			columnsAndDataTypes.put(columnDefName + i, "");
		}
		return delimiter;
	}
    
    private static String fromFirstLineGetColumns(LinkedHashMap<String, String> columnsAndDataTypes, String dataRow, HashMap<String, String> paramsInCS) 
	{
		String[] delimitersFromConfig = paramsInCS.get("csvDelimiters").split("#");
		String delimiter = ",";
		for (String sDelimiter : delimitersFromConfig) {           
		    //Do your stuff here
			if(dataRow.contains(sDelimiter))
			{
				delimiter = sDelimiter;
			}
		}

		String[] dataArray = dataRow.split(delimiter);
		for(int i = 0; i < dataArray.length; i++)
		{
			String currColumn = removeDquotes(dataArray[i]);
			//listColumns.add(currColumn);
			columnsAndDataTypes.put(currColumn, "");
		}
		return delimiter;
	}
    
    private static String removeDquotes(String currColumnFrom) {
		String currColumn;
		if(currColumnFrom.startsWith("\"") && currColumnFrom.endsWith("\""))
		{
			currColumn = currColumnFrom.substring(1, currColumnFrom.length()-1);
			//System.out.println("Q & !Q::" + currColumnFrom + ":"+ currColumn);
			outputLogin(bVerbose, "Q & !Q::" + currColumnFrom + ":"+ currColumn);
			
		}
		else
		{currColumn = currColumnFrom;}
		return currColumn;
	}
    
    private static String evaluateDataTypesFromColumns(HashMap<String, String> columnsAndDataTypes, List<String> listOfColumns, boolean isDateValdated,
			String[] dataArray, HashMap<String, String> propertyParams) 
	{	
		String sDateFormat = "";
		String returnDateFormat = "";
		for(int i = 0; i < dataArray.length; i++)
		{	
			String currColumn = removeDquotes(dataArray[i]);
			Scanner input = new Scanner(currColumn);
			outputLogin("dataArray[i]::" + currColumn);

			if (input.hasNextInt())
			{
				outputLogin(currColumn + "::"+ "This input is of type Integer.");
				//listTypes.add("Integer");
				String colType = columnsAndDataTypes.get(listOfColumns.get(i));
				if(colType != "FLOAT")
				{
					columnsAndDataTypes.put(listOfColumns.get(i), "INTEGER");
				}
			}
			else if (input.hasNextFloat())
			{ 
				outputLogin(currColumn + "::"+"This input is of type Float.");
				////listTypes.add("Float");
				String colType = columnsAndDataTypes.get(listOfColumns.get(i));
				if(colType != "FLOAT")
				{
					columnsAndDataTypes.put(listOfColumns.get(i), "FLOAT");
				}
			}
			else if (input.hasNextBoolean())
			{
				outputLogin(currColumn + "::"+"This input is of type Boolean.");  
				////listTypes.add("BOOLEAN");
				columnsAndDataTypes.put(listOfColumns.get(i), "BOOLEAN");
			}
			else  if (currColumn.contains("/") || currColumn.contains("-"))
			{
				//need to check what dateType need to process
				isDateValdated = false;
				sDateFormat = validateDateColumn(currColumn, propertyParams);
				
				if(sDateFormat != "empty")
					isDateValdated = true;
				//outputLogin(date.toString() + "::"+"This input is of type DATE.");
				if(isDateValdated)
				{
					////listTypes.add("DATE");
					columnsAndDataTypes.put(listOfColumns.get(i), "DATE");
					returnDateFormat = sDateFormat;
				}
				else 
				{
					////listTypes.add("VARCHAR");
					columnsAndDataTypes.put(listOfColumns.get(i), "VARCHAR");
				}
				
			}
			else if (input.hasNextLine() && !isDateValdated)
			{
				outputLogin(dataArray[i] + "::"+"This input is of type string."); 
				////listTypes.add("VARCHAR");
				columnsAndDataTypes.put(listOfColumns.get(i), "VARCHAR");
			}
			input.close();
			
		}
		return returnDateFormat;
	}
    
    private static String validateDateColumn(String currColumn, HashMap<String, String> propertyParams) {	
		String[] dateFormat = propertyParams.get("dateFormat").split("#");
		String sOutFormat = "empty";
		
		for (String sFormat: dateFormat) {           
			//Do your stuff here
			outputLogin(sFormat); 
			SimpleDateFormat sDataFormat = new SimpleDateFormat(sFormat);//dd-MM-YYYY
			boolean isDateValdated = false;
			//if(!isDateValdated)
			
			isDateValdated = parseDateWithParam(currColumn, sDataFormat);
			if(isDateValdated && sOutFormat.equals("empty"))
				sOutFormat = sFormat;
			
		}

		return sOutFormat;
	}
    
    private static boolean parseDateWithParam(String currColumn, SimpleDateFormat formatDate) {
		boolean isDateValdated;
		try {
			isDateValdated = true;
			formatDate.parse ( currColumn );

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			outputLogin(bVerbose, currColumn + "::"+"This input is of type 1NOT DATE.");
			isDateValdated = false;
		}
		return isDateValdated;
	}
    
    

	private static HashMap<String, String> getFileFromS3(String fileNameWithPath, HashMap<String, String> connParams) throws IOException, FileNotFoundException {
		
		//https://s3.amazonaws.com/my-first-s3-bucket-ika/star0000-1.csv
		HashMap<String, String> paramsInSql = new HashMap<String, String>();
		
		AmazonS3 s3 = getAwsS3Object(connParams);
		
		String fileExtention = getExtension(fileNameWithPath);
		
		if(fileNameWithPath.startsWith("/"))
			fileNameWithPath = fileNameWithPath.replaceFirst("/", "");
		
		String[] sFirstPositionAsBucket = fileNameWithPath.split("/");
        String bucketName = sFirstPositionAsBucket[0];// fileNameWithPath.replace("s3.amazonaws.com", "").replace(fileName, "");// "my-first-s3-bucket-" + "ika";// + UUID.randomUUID();
        String key = fileNameWithPath.replace(bucketName + "/", "");//fileName;//"star0000-1.csv";//"MyObjectKey";

        paramsInSql.put("bucketName", bucketName);
        paramsInSql.put("key", key); 
        paramsInSql.put("fileExtention", fileExtention);   
        
        try {
        	//
            GetObjectRequest objReq = new GetObjectRequest(bucketName, key);
            objReq.setRange(0, 2048);// = 111;
            S3Object s3object = s3.getObject(objReq);
            int iCountLines = 10;
            
            File file = File.createTempFile("temp_CSV_", ".csv");
            file.deleteOnExit();
            Writer writer = new OutputStreamWriter(new FileOutputStream(file));
            if(fileExtention.equalsIgnoreCase("gz") || fileExtention.equalsIgnoreCase("gzip"))
            {
            	gzip2File(s3object, writer,fileExtention);// save data from GZIP to File
            }
            if(fileExtention.equalsIgnoreCase("csv"))
            {
            	justCsv2File(s3object, iCountLines, writer);// save data from CSV to File
            }
			/* zip is not Supported yet
			 * if(fileExtention.equalsIgnoreCase("zip")) { zip2File(s3object,
			 * writer,fileExtention); }
			 */
            writer.close();
            paramsInSql = processCSV(paramsInSql, file, fileNameWithPath, connParams);
           
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
		return paramsInSql;
	}

	private static AmazonS3 getAwsS3Object(HashMap<String, String> connParams) {
		AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
            connParams.put("accessKey", credentials.getAWSAccessKeyId());
            connParams.put("secretKey", credentials.getAWSSecretKey());
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/Users/ikarbovskyy/.aws/credentials), and is in valid format.",
                    e);
        }

        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(connParams.get("regionS3"))
            .build();
		return s3;
	}

	private static void justCsv2File(S3Object s3object, int iCountLines, Writer writer) throws IOException {
		BufferedReader in = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
		String line;
		int iCount = 0;
            
		while ((line = in.readLine()) != null && (iCount < iCountLines))  {
			System.out.println("process line"+iCount+"::" + line);
			writer.write(line + "\n");
			iCount++;
		}
	}

	private static void gzip2File(S3Object s3object, Writer writer, String fileExtention) throws IOException, UnsupportedEncodingException {
		byte[] buffer = new byte[1024];
		BufferedInputStream bufferedInputStream;
		bufferedInputStream = new BufferedInputStream( new GZIPInputStream(s3object.getObjectContent()));
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		int length;
		 
		try {
			while ((length = bufferedInputStream.read(buffer)) != -1) {
				result.write(buffer, 0, length);
				writer.write(result.toString("UTF-8"));
			}
		} catch (Exception e) {
			// Suppressing error on partial file
			System.out.println("from GZip processing::" + e.toString());
		}
	}
	
	/* zip is not supported on Snowflake yet
	 * private static void zip2File(S3Object s3object, Writer writer, String
	 * fileExtention) throws IOException, UnsupportedEncodingException { byte[]
	 * buffer = new byte[1024]; BufferedInputStream bufferedInputStream;
	 * 
	 * 
	 * ZipInputStream zipstream = new ZipInputStream(s3object.getObjectContent());
	 * ZipEntry entry; try { while((entry = zipstream.getNextEntry())!=null) {
	 * ByteArrayOutputStream result = new ByteArrayOutputStream();
	 * 
	 * int len; while ((len = zipstream.read(buffer)) > 0) { result.write(buffer, 0,
	 * len); writer.write(result.toString("UTF-8"));
	 * System.out.println(result.toString("UTF-8")); } } } catch (Exception e) { //
	 * TODO Auto-generated catch block //e.printStackTrace();
	 * System.out.println("from Zip processing::" + e.toString()); } }
	 */
    private static HashMap<String, String> getProperties() throws FileNotFoundException, IOException {
		Properties prop = new Properties();
		InputStream input = new FileInputStream("config.properties");
		// load a properties file
		prop.load(input);
		HashMap<String, String> connParams = new HashMap<String, String>();
		// get the property value and print it out
		try {
			bVerbose = Boolean.parseBoolean(prop.getProperty("verbose"));
			columnDefName = prop.getProperty("columnDefName");
			iHowManyLines2check = Integer.parseInt(prop.getProperty("howManyLines2check"));
			bProcessHeaderFirstLine = Boolean.parseBoolean(prop.getProperty("processHeaderFirstLine"));
			connParams.put("userSF",prop.getProperty("userSF"));
			connParams.put("passwordSF",prop.getProperty("passwordSF"));
			connParams.put("warehouseSF", prop.getProperty("warehouseSF"));
			connParams.put("dbSF",prop.getProperty("dbSF"));
			connParams.put("schemaSF",prop.getProperty("schemaSF"));
			connParams.put("regionSF",prop.getProperty("regionSF"));
			connParams.put("accountSF",prop.getProperty("accountSF"));
			connParams.put("dateFormat",prop.getProperty("dateFormat"));
			connParams.put("csvDelimiters",prop.getProperty("csvDelimiters"));	
			connParams.put("regionS3",prop.getProperty("regionS3"));	
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			outputLogin(true, "Incorrect congif settings::" + e.toString());
			//return;
		}
		return connParams;
	}

    
    private static Connection getConnection(HashMap<String, String> connParams)
			throws SQLException {
		try {
			Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
		} catch (ClassNotFoundException ex) {
			System.err.println("Driver not found");
		}
		// testHashMap2.get("key1")
		Properties properties = new Properties();
		properties.put("user", connParams.get("userSF"));        // replace "" with your user name
		properties.put("password", connParams.get("passwordSF"));    // replace "" with your password
		properties.put("account", connParams.get("accountSF"));     // replace "" with your account name
		properties.put("warehouse", connParams.get("warehouseSF"));   // replace "" with target warehouse name
		properties.put("db", connParams.get("dbSF"));          // replace "" with target database name
		properties.put("schema", connParams.get("schemaSF"));      // replace "" with target schema name
		String regionSF = connParams.get("regionSF");
		String regionSFlink = "";
		if(!regionSF.equals("default"))
		{
			properties.put("region", connParams.get("regionSF"));
			regionSFlink = "." + regionSF;
		}   
		// replace <account_name> with the name of your account, as provided by Snowflake
		// replace <region_id> with the name of the region where your account is located (if not US West)
		// remove region ID segment (not needed) if your account is located in US West
		String connectStr = "jdbc:snowflake://"+connParams.get("accountSF")+ regionSFlink + ".snowflakecomputing.com";
		return DriverManager.getConnection(connectStr, properties);
	}
    
    private static String stripExtension (String fileNameWithPath) {

		Path p = Paths.get(fileNameWithPath);
		String fileName = p.getFileName().toString();
		// Handle null case specially.
		if (fileName == null) return null;
		// Get position of last '.'.
		int pos = fileName.lastIndexOf(".");
		// If there wasn't any '.' just return the string as is.
		if (pos == -1) return fileName;
		// Otherwise return the string, up to the dot.
		return fileName.substring(0, pos);
	}
    
    public static String getExtension(String fileName) {
		char ch;
		int len;
		if(fileName==null || 
				(len = fileName.length())==0 || 
				(ch = fileName.charAt(len-1))=='/' || ch=='\\' || //in the case of a directory
				ch=='.' ) //in the case of . or ..
			return "";
		int dotInd = fileName.lastIndexOf('.'),
				sepInd = Math.max(fileName.lastIndexOf('/'), fileName.lastIndexOf('\\'));
		if( dotInd<=sepInd )
			return "";
		else
			return fileName.substring(dotInd+1).toLowerCase();
	}


}
