package cat.iesesteveterradas;

import java.io.FileWriter;
import java.io.IOException;

import org.basex.api.client.ClientSession;
import org.basex.core.*;
import org.basex.core.cmd.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

public class PR32CreateMain {
    private static final Logger logger = LoggerFactory.getLogger(PR32CreateMain.class);    

    public static void main(String[] args) throws IOException {
         // Initialize connection details
        String host = "127.0.0.1";
        int port = 1984;
        String username = "admin"; // Default username
        String password = "admin"; // Default password

        // Establish a connection to the BaseX server
        try (ClientSession session = new ClientSession(host, port, username, password)) {
            logger.info("Connected to BaseX server.");

            session.execute(new Open("sports.meta.stackexchange")); 
            
            String myQuery = """
                declare option output:method "json";
                declare option output:indent "yes";
                let $postsWithMoreViews := (
                  for $post in /posts/row[@PostTypeId='1']
                  order by xs:integer($post/@ViewCount) descending
                  return $post
                )[position() <= 100]

                return
                map {
                  "question": array {
                    for $post in $postsWithMoreViews
                    return
                      map {
                        "Id": string($post/@Id),
                        "PostTypeId": string($post/@PostTypeId),
                        "AcceptedAnswerId": string($post/@AcceptedAnswerId),
                        "CreationDate": string($post/@CreationDate),
                        "Score": string($post/@Score),
                        "ViewCount": string($post/@ViewCount),
                        "Body": string($post/@Body),
                        "OwnerUserId": string($post/@OwnerUserId),
                        "LastActivityDate": string($post/@LastActivityDate),
                        "Title": string($post/@Title),
                        "Tags": string($post/@Tags),
                        "AnswerCount": string($post/@AnswerCount),
                        "CommentCount": string($post/@CommentCount),
                        "ContentLicense": string($post/@ContentLicense)
                      }
                  }
                }
                
            """;

            // Execute the query
            String result = session.execute(new XQuery(myQuery));
            // Print the result
            logger.info("Query Result:");
            logger.info(result);
            // Save the result
            FileWriter fileWriter = new FileWriter("./data/out/result.json");
            fileWriter.write(result);
            fileWriter.close();

        } catch (BaseXException e) {
            logger.error("Error connecting or executing the query: " + e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        String resultFilePath = "./data/out/result.json";
        String resultContent = FileUtils.readFileToString(resultFilePath);

        // Parse the JSON string into a JSONObject
        JSONObject jsonResult = new JSONObject(resultContent);
        JSONArray questionArray = jsonResult.optJSONArray("question");
        for (int i=0; i < questionArray.length();i++) {
            JSONObject question = questionArray.optJSONObject(i);
            MongoUtils.uploadToMogoDB(question.optString("Id"), 
                question.optString("PostTypeId"), 
                question.optString("AcceptedAnswerId"), 
                question.optString("CreationDate"), 
                question.optString("Score"), 
                question.optString("ViewCount"), 
                question.optString("Body"), 
                question.optString("OwnerUserId"), 
                question.optString("LastActivityDate"), 
                question.optString("Title"), 
                question.optString("Tags"), 
                question.optString("AnswerCount"), 
                question.optString("CommentCount"), 
                question.optString("ContentLicense")); 
        }
        logger.info("Insertions done");
    }2
}
