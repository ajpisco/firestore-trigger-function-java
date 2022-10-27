/*
    In order to test the service we need to set the authentication:
    set GOOGLE_APPLICATION_CREDENTIALS=<Path to the Service account>
 */

package com.example;

import com.google.common.testing.TestLogHandler;
import com.google.common.truth.Truth;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.cloud.functions.Context;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExampleTest {

  // Loggers + handlers for various tested classes
  // (Must be declared at class-level, or LoggingHandler won't detect log records!)
  private static final Logger logger = Logger.getLogger(Example.class.getName());

  private static final TestLogHandler LOG_HANDLER = new TestLogHandler();

  private static final Gson gson = new Gson();

  private static final boolean dryRun = false;
  private static final String projectId = "ajpiscopro2";
  private static final String collectionId = "mycol";
  private static final String documentId = "K1rcTkvD2cpLVtwFw4uz";
  private static final String firestoreEventTemplate = "{"
    + "    \"oldValue\": {"
    + "        \"fields\": {},"
    + "        \"name\": \"projects/projectId/databases/(default)/documents/collectionId/documentId\""
    + "    },"
    + "    \"updateMask\": {"
    + "        \"fieldPaths\": []"
    + "    },"
    + "    \"value\": {"
    + "        \"fields\": {},"
    + "        \"name\": \"projects/projectId/databases/(default)/documents/collectionId/documentId\""
    + "    }"
    + "}";

  @BeforeClass
  public static void beforeClass() {
    logger.addHandler(LOG_HANDLER);
  }

  @After
  public void afterTest() {
    LOG_HANDLER.clear();
  }

  @Test
  public void triggerDocumentCreationTest() {
    try {
        final String JSON_FILE="src\\test\\java\\com\\example\\NewFirestoreDocument.json";
        Reader reader = Files.newBufferedReader(Paths.get(JSON_FILE));
        JsonObject document = gson.fromJson(reader, JsonObject.class);

        logger.info("New document: " + document);

        MockContext context = new MockContext();
        context.resource = "resource_1";
        context.eventType = "event_type_2";

        // Parse Firestore event
        String firestoreEventStr = firestoreEventTemplate
                                    .replaceAll("projectId",projectId)
                                    .replaceAll("collectionId",collectionId)
                                    .replaceAll("documentId",documentId);
        JsonObject firestoreEventJson = gson.fromJson(firestoreEventStr, JsonObject.class);
        // Add dry run flag
        firestoreEventJson.addProperty("DryRun", String.valueOf(dryRun));
        // Add nested object
        JsonObject valuesItem = firestoreEventJson.get("value").getAsJsonObject();
        valuesItem.add("fields", transformJsonObjectToFirestore(document));
        firestoreEventJson.add("value", valuesItem);

        new Example().accept(firestoreEventJson.toString(), context);

        List<LogRecord> logs = LOG_HANDLER.getStoredLogRecords();

        // Test if document was encrypted as expected
        // TODO

        // Test if service was called successfully
        // TODO

        // Test if firestore values was updated
        Truth.assertThat(logs.get(dryRun? 6:5).getMessage()).matches("(.*)Document(.*)updated at(.*)");
    }
    catch(Exception ex){
        logger.severe("Error while running triggerDocumentCreationTest: " + ex.getMessage());
    }
  }

  // This recursive method will receive the json document and transform it to the firestore json format
  JsonObject transformJsonObjectToFirestore(JsonObject source){
    JsonObject jObject = new JsonObject();
    Set<Entry<String, JsonElement>> sourceSet = source.entrySet();
    for (Entry<String, JsonElement> entry : sourceSet) {
        jObject.add(entry.getKey(), evaluateJsonObject(entry.getValue()));
    }
    return jObject;
  }

  // This recursive method will evaluate the type of the received object and return its corresponding value
  JsonObject evaluateJsonObject(JsonElement source){
    JsonObject jObject = new JsonObject();
    // Check if element is primitive (string, number or boolean)
    // and add the corresponding key
    if(source.isJsonPrimitive()){
        if(source.getAsJsonPrimitive().isBoolean()){
            jObject.add("booleanValue", source.getAsJsonPrimitive());
            return jObject;
        }
        else if(source.getAsJsonPrimitive().isNumber()){
            jObject.add("integerValue", source.getAsJsonPrimitive());
            return jObject;
        }
        else if(source.getAsJsonPrimitive().isString()){
            jObject.add("stringValue", source.getAsJsonPrimitive());
            return jObject;
        }
    }
    // If element is an array we need to iterate over all values
    // and check for their type by calling this methos
    else if(source.isJsonArray()){
        JsonArray jObjectArray = new JsonArray();
        JsonArray jArray = source.getAsJsonArray();
        for(JsonElement jArrayElement : jArray){
            JsonElement jElement = evaluateJsonObject(jArrayElement);
            jObjectArray.add(jElement);
        }
        JsonObject valuesObject = new JsonObject();
        valuesObject.add("values", jObjectArray);
        jObject.add("arrayValue", valuesObject);
        return jObject;
    }
    // If element is a map we need to go over the map just like in the root map
    else if(source.isJsonObject()){
        JsonObject jObjectMap = transformJsonObjectToFirestore(source.getAsJsonObject());
        JsonObject fieldsObject = new JsonObject();
        fieldsObject.add("fields", jObjectMap);
        jObject.add("mapValue", fieldsObject);
        return jObject;
    }
    return null;
  }
}


// Class that mocks Cloud Functions "context" objects
// Used to create fake context objects for function tests
class MockContext implements Context {
  public String eventId;
  public String eventType;
  public String timestamp;
  public String resource;

  @Override
  public String eventId() {
    return this.eventId;
  }

  @Override
  public String timestamp() {
    return this.timestamp;
  }

  @Override
  public String eventType() {
    return this.eventType;
  }

  @Override
  public String resource() {
    return this.resource;
  }
}