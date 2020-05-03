package mongodb_workshop;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.BasicDBObject;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.lt;


import org.json.JSONObject;

import org.bson.Document;
import org.bson.conversions.Bson;

public class MainLab {

	public static void main(String[] args) {
	

		// connect to MongoDB on local
		MongoClient mongoClient = MongoClients.create("mongodb://127.0.0.1:27017/");

		// connect to database in this case "test"
		// note  If a database does not exist, MongoDB creates the database when you first store data for that database.
		MongoDatabase database = mongoClient.getDatabase("test");

		// connect to collection aka table in this case "people"
		MongoCollection<Document> collection = database.getCollection("people");
		

		// **CREATE********************************************************************
		// **CREATE********************************************************************
		// **CREATE********************************************************************
		// creating a new document
		// Example:

		Document createdDoc = new Document("user_id", "pmv893").append("age", 44).append("status", "C");
		// more append examples
		// .append("status", Arrays.asList("v3.2", "v3.0", "v2.6"))
		// .append("info", new Document("x", 203).append("y", 102));

		collection.insertOne(createdDoc);
//		 
		// Example add many documents:
//		 List<Document> documents = new ArrayList<Document>();
//		 for (int i = 0; i < 100; i++) {
//		     documents.add(new Document("i", i));
//		 }
		// collection.insertMany(documents);
		// **CREATE********************************************************************
		// **CREATE********************************************************************
		// **CREATE********************************************************************

		// **READ********************************************************************
		// **READ********************************************************************
		// **READ********************************************************************
		// loop through all the documents, aka rows in the collection and display to the
		// screen
		MongoCursor<Document> cursor = collection.find().iterator();
		try {
			while (cursor.hasNext()) {
				String documentString = cursor.next().toJson();

				System.out.println(documentString);

				// JSON Parse Example

				JSONObject obj = new JSONObject(documentString);
				String userID = obj.getString("user_id");
				String mongoID = obj.getJSONObject("_id").getString("$oid");

				System.out.println("the user ID is " + userID);
				System.out.println("the mongo generated ID is " + mongoID);

//loop through array of post example
//			    	JSONArray arr = obj.getJSONArray("posts");
//			    	for (int i = 0; i < arr.length(); i++)
//			    	{
//			    	    String post_id = arr.getJSONObject(i).getString("post_id");
//			    	    
//			    	}

			}
		}

		finally {
			cursor.close();
		}

		// display the number of documents in the collection
		System.out.println("documents: " + collection.countDocuments());

		// display the first document in the collection
		Document myDoc = collection.find().first();
		System.out.println("First document:");
		System.out.println(myDoc.toJson());

		// display all document where status is A
		System.out.println("display all document where status is A: ");
		Bson bsonFilter0 = Filters.eq("status", "A");
		FindIterable<Document> findIt = collection.find(bsonFilter0);

		for (Document doc : findIt) {
			System.out.println(doc.toJson());
		}

		System.out.println("display all document where age is greater than 11 and less than 55");
		Bson bsonFilter = Filters.gt("age", 11);
		Bson bsonFilter2 = Filters.lt("age", 55);

		FindIterable<Document> findIt2 = collection.find(and(bsonFilter, bsonFilter2));

		for (Document doc : findIt2) {
			System.out.println(doc.toJson());
		}
		// **READ********************************************************************
		// **READ********************************************************************
		// **READ********************************************************************

		// **UPDATE********************************************************************
		// **UPDATE********************************************************************
		// **UPDATE********************************************************************

		// update a single document.

		BasicDBObject set = new BasicDBObject("$set", new BasicDBObject("age", 110));

		// update the first document with age 10 to 110

		collection.updateOne(eq("age", 10), set);

		System.out.println("update the first document with age 10 to 110: ");
//				
		// print
		// MongoCursor<Document> cursor2 = collection.find().iterator();
//					try {
//					    while (cursor2.hasNext()) {
//					        System.out.println(cursor2.next().toJson());
//					    }
//					}
//				
//					finally {
//						cursor2.close();
//					}

		// update all documents with age 55 to 110:
		BasicDBObject set2 = new BasicDBObject("$set", new BasicDBObject("age", 110));

		UpdateResult updateResult = collection.updateMany(eq("age", 55), set2);

		System.out.println("update all documents with age 55 to 110: ");
//					//print
		// MongoCursor<Document> cursor3 = collection.find().iterator();
//						try {
//						    while (cursor3.hasNext()) {
//						        System.out.println(cursor3.next().toJson());
//						    }
//						}
//					
//						finally {
//							cursor3.close();
//						}
		// **UPDATE********************************************************************
		// **UPDATE********************************************************************
		// **UPDATE********************************************************************

		// **DELETE********************************************************************
		// **DELETE********************************************************************
		// **DELETE********************************************************************
		// delete document where usser_id is xyz123
		collection.deleteOne(eq("user_id", "xyz123"));
		System.out.println("delete document where user_id is xyz123");
		MongoCursor<Document> cursor3 = collection.find().iterator();
		try {
			while (cursor3.hasNext()) {
				System.out.println(cursor3.next().toJson());
			}
		}

		finally {
			cursor3.close();
		}
		// **DELETE********************************************************************
		// **DELETE********************************************************************
		// **DELETE********************************************************************

	}

}
