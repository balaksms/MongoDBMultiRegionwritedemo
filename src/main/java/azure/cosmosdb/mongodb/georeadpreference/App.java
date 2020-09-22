package azure.cosmosdb.mongodb.georeadpreference;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.client.MongoCollection;

public class App {
	private String  appConfigPath = "src/main/resources/config";
	private String connectionString = "";
	private String readTargetRegion = "";
	private String dbName = "";
	private String collName = "";
	private Properties properties;
	private MongoClient client;

	App() {
		this.properties = new Properties();
		try {
			this.properties.load(new FileInputStream(appConfigPath));
			this.connectionString = this.properties.getProperty("connectionString");
			this.readTargetRegion = this.properties.getProperty("readTargetRegion");
			this.dbName = this.properties.getProperty("databaseName");
			this.collName = this.properties.getProperty("collectionName");
			if(this.connectionString.isEmpty())	{
				System.out.println("Connection string is missing!");
				throw new IllegalArgumentException("connectionString");
			}
			else if(this.readTargetRegion.isEmpty()) {
				System.out.println("Target read region for tags is missing!");
				throw new IllegalArgumentException("readTargetRegion");
			}
			else if(this.dbName.isEmpty()) {
				System.out.println("Database name is missing!");
				throw new IllegalArgumentException("databaseName");
			}
			else if(this.collName.isEmpty()) {
				System.out.println("Collection name is missing!");
				throw new IllegalArgumentException("collectionName");
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void InsertDocs() {
		System.out.println("Inserting documents");
		MongoCollection<Document> coll = this.client.getDatabase(this.dbName).getCollection(this.collName);
		for(int i=0; i<100000; i++) {
			coll.insertOne(new Document("x", i));
			System.out.println("Inserting document." + i);
		}
		System.out.println("Inserting documents complete.");
	}

	private void ReadFromPrimaryRegion() {
		System.out.println("Reading documents from Primary");
		MongoCollection<Document> coll = this.client.getDatabase(this.dbName).getCollection(this.collName);
		int cnt=0;
		for(Document doc : coll.find())	{
			cnt++;
		}
		System.out.println("Docs read from Write region: "+cnt);
	}

	private void ReadFromSecondaryRegion() {
		System.out.println("Reading documents from Secondary");
		MongoCollection<Document> coll = this.client.getDatabase(this.dbName).getCollection(this.collName).withReadPreference(ReadPreference.secondaryPreferred());
		int cnt=0;
		for(Document doc : coll.find())	{
			cnt++;
		}
		System.out.println("Docs read from read region if present : "+cnt);
	}

	private void ReadFromNearestRegion() {
		System.out.println("Reading documents from Nearest");
		MongoCollection<Document> coll = this.client.getDatabase(this.dbName).getCollection(this.collName).withReadPreference(ReadPreference.nearest());
		int cnt=0;
		for(Document doc : coll.find())	{
			cnt++;
		}
		System.out.println("Docs read from Nearest region : "+cnt);
	}

	private void ReadFromSpecificRegion(String regionName) {
		List<TagSet> tgsetList = new ArrayList<TagSet>();
		TagSet tgset = new TagSet(new Tag("region", regionName));
		tgsetList.add(tgset);
		System.out.println("Reading documents from region: "+ regionName);
		MongoCollection<Document> coll = this.client.getDatabase(this.dbName).getCollection(this.collName).withReadPreference(ReadPreference.secondaryPreferred(tgsetList));
		int cnt=0;
		for(Document doc : coll.find())	{
			cnt++;
		}
		System.out.println("Docs read from specified region : "+cnt);
	}

	private void InitializeMongoClient() {
		MongoClientOptions.Builder optionsBuilder = new MongoClientOptions.Builder();
		// Set values to prevent timeouts
		optionsBuilder.socketTimeout(10000);
		optionsBuilder.maxConnectionIdleTime(60000);
		optionsBuilder.heartbeatConnectTimeout(5000);



		MongoClientURI mongoClientURI = new MongoClientURI(this.connectionString, optionsBuilder);


		this.client = new MongoClient(mongoClientURI);
	}

	public void RunSample()	{
		System.out.println("Start sample run..");
		//Initialize Mongo Client
		InitializeMongoClient();

		//Insert docs
		this.InsertDocs();

		try {
			//If the collection is not already present, new collection is created in the background and
			//inserts happen. then they are replicated to read regions. Hence, before the first read request
			//we sleep for this process to complete.
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

//		//Read using different read preference modes
//		this.ReadFromPrimaryRegion();
//		this.ReadFromSecondaryRegion();
//		this.ReadFromNearestRegion();
		this.ReadFromSpecificRegion(this.readTargetRegion);
		System.out.println("Sample run completed..");
	}

	public static void main( String[] args ) {
		App sampleApp = new App();
		sampleApp.RunSample();
	}
}
