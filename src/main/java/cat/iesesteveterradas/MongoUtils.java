package cat.iesesteveterradas;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;

public class MongoUtils {
    public static void uploadToMogoDB(String Id, String PostTypeId, String AcceptedAnswerId, String CreationDate, String Score, String ViewCount, String Body, String OwnerUserId, String LastActivityDate, String Title, String Tags, String AnswerCount, String CommentCount, String ContentLicense) {
        // Connectar-se a MongoDB (substitueix amb la teva URI de connexió)
        try (var mongoClient = MongoClients.create("mongodb://root:example@localhost:27017")) {
            MongoDatabase database = mongoClient.getDatabase("yourDatabaseName");
            MongoCollection<Document> collection = database.getCollection("yourCollectionName");

            // Crear un nou document
            Document question = new Document("Id", Id)
                                .append("PostTypeId", PostTypeId)
                                .append("AcceptedAnswerId", AcceptedAnswerId)
                                .append("CreationDate", CreationDate)
                                .append("Score", Score)
                                .append("ViewCount", ViewCount)
                                .append("Body", Body)
                                .append("OwnerUserId", OwnerUserId)
                                .append("LastActivityDate", LastActivityDate)
                                .append("Title", Title)
                                .append("Tags", Tags)
                                .append("AnswerCount", AnswerCount)
                                .append("CommentCount", CommentCount)
                                .append("ContentLicense", ContentLicense);

            // Inserir el document a la col·lecció
            collection.insertOne(question);

        } catch (Exception e) {
            System.err.println("An error occurred: " + e.getMessage());
        }
    }
}
