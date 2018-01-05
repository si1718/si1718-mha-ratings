package data.streaming.utils;


import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;

import data.streaming.dto.Project;
import data.streaming.dto.Rating;

public class MongoUtils {
	
	private static final String MONGO_DB = "mongodb://manuel:manuel@ds255455.mlab.com:55455/si1718-mha-projects";
	private static final String MONGO_CL = "projects";
	private static final String MONGO_RT = "ratings";
	
	public static MongoDatabase database= null; 
	
	public static void initialize() {
		CodecRegistry pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));
		
		MongoClientURI uri = new MongoClientURI(MONGO_DB);
		@SuppressWarnings("resource")
		MongoClient mongoClient = new MongoClient(uri);
		database = mongoClient.getDatabase(uri.getDatabase()).withCodecRegistry(pojoCodecRegistry);
	}
	
	public static List<String> getKeywords() {
		List<String> keywords = new ArrayList<String>();
		MongoCollection<Project> collection = database.getCollection(MONGO_CL, Project.class);
		MongoCursor<Project> cursor = collection.find().iterator();
		
		try {
		    while (cursor.hasNext()) {
		    	for(String keyword: cursor.next().getKeywords()) {
		    		if(!keywords.contains(keyword) && !keyword.trim().isEmpty()) {
		    			keywords.add(keyword);
		    		}
		    	}
		    }
		} finally {
		    cursor.close();
		}
		
		return keywords;
	}

	public static List<Rating> getRatings() {
		List<Rating> ratings = new ArrayList<Rating>();
		List<Project> projects = new ArrayList<Project>();
		MongoCollection<Project> collection = database.getCollection(MONGO_CL, Project.class);
		MongoCursor<Project> cursor = collection.find().iterator();
		
		try {
			Project project = null;
		    while (cursor.hasNext()) {
		    	project =  cursor.next();
	    		if(!project.getKeywords().isEmpty()) {
	    			projects.add(project);
	    		}
		    }
		    
		    Double score = 0.0;
		    Project one = null;
		    Project two = null;
		    for(int i=0; i<projects.size(); i++) {
		    	one = projects.get(i);
		    	for(int j=i+1; j<projects.size(); j++) {
		    		two = projects.get(j);
		    		score = 0.0;
		    		for(String keyOne: one.getKeywords()) {
		    			for(String keyTwo: two.getKeywords()) {
		    				if(keyOne.equals(keyTwo)) {
		    					score++;
		    				}
		    			}
		    		}
		    		if(score > 0) {
		    			score = (score * 5)/two.getKeywords().size();
		    			ratings.add(new Rating(one.getIdProject(), two.getIdProject(), score));
		    		}
		    	}
		    }
		    
		} finally {
		    cursor.close();
		}
		return ratings;
	}

	private static Rating addRating(Rating x) {
		MongoCollection<Rating> collection = database.getCollection(MONGO_RT, Rating.class);
		BasicDBObject query = new BasicDBObject();
		query.append("idProject1", x.getIdProject1());
		query.append("idProject2", x.getIdProject2());
		
		try {
			collection.findOneAndUpdate(query, new Document("$set", x), (new FindOneAndUpdateOptions()).upsert(true));
		} catch (Exception e) {}
		
		return x;
	}
	
	public static void addRatings() {
		getRatings().stream().forEach(x -> addRating(x));
	}
}
