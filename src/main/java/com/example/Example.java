/*

To deploy function:
gcloud functions deploy firestore-trigger-function --region europe-west2 --entry-point com.example.Example --runtime java17 --trigger-event "providers/cloud.firestore/eventTypes/document.create" --trigger-resource "projects/ajpiscopro2/databases/(default)/documents/mycol/{doc_id}"

*/

package com.example;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.WriteResult;
import com.google.cloud.functions.Context;
import com.google.cloud.functions.RawBackgroundFunction;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class Example implements RawBackgroundFunction {
  private static final Logger logger = Logger.getLogger(Example.class.getName());

  // Use GSON (https://github.com/google/gson) to parse JSON content.
  private static final Gson gson = new Gson();

  private static final FirestoreOptions firestoreOptions =
  FirestoreOptions.getDefaultInstance()
      .toBuilder()
      .build();
  private static Firestore db = firestoreOptions.getService();
  private static int documentIdPosition = 6;
  private static int collectionIdPosition = 5;

  @Override
  public void accept(String json, Context context) throws IOException, InterruptedException, ExecutionException {
    JsonObject body = gson.fromJson(json, JsonObject.class);
    logger.info("Message: " + body);
    logger.info("Function triggered by event on: " + context.resource());
    logger.info("Event type: " + context.eventType());

    // Check if dry run mode is set
    boolean dryRun = false;
    if (body.has("DryRun")) {
        dryRun = body.get("DryRun").getAsBoolean();
    }
    if(dryRun){
        logger.info("Dry run mode activated");
    }

    // Encoding document Stage
    // Transform Firestore document to json
    JsonObject document = body.get("value").getAsJsonObject().get("fields").getAsJsonObject();
    document = transformFirestoreToJsonObject(document);
    logger.info("Stringed document:" + document);
    // TODO: Add logic to encrypt the document and call out a service

    // Update the document with status success
    // Firestore location should be: projects/projectId/databases/(default)/documents/collectionId/documentId
    String firestoreItemLocation = body.get("value").getAsJsonObject().get("name").getAsString();
    String[] splitLocation = firestoreItemLocation.split("/");
    String collectionId = splitLocation[collectionIdPosition];
    String documentId = splitLocation[documentIdPosition];

    if(dryRun){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        logger.info(String.format("[DRY-RUN] Document %s/%s updated at %s", collectionId, documentId, sdf.format(timestamp)));
    }
    else{
        DocumentReference docRef = db.collection(collectionId).document(documentId);
    
        ApiFuture<WriteResult> future = docRef.update("status", true);
        WriteResult result = future.get();
        logger.info(String.format("Document %s/%s updated at %s", collectionId, documentId, result.getUpdateTime()));
    }
  }

  // This recursive method will receive the firestore json format and transform it to a simple json format
  JsonObject transformFirestoreToJsonObject(JsonObject source){
    JsonObject jObject = new JsonObject();
    Set<String> sourceKeysSet = source.keySet();
    for (String s : sourceKeysSet) {
        JsonElement jsonElement = evaluateJsonObject(source.get(s).getAsJsonObject());
        if(jsonElement != null){
            jObject.add(s, jsonElement);
        }
    }
    return jObject;
  }

  // This recursive method will evaluate the type of the received object and return its corresponding value
  JsonElement evaluateJsonObject(JsonObject source){
    // Check source value type and get its value
    // Example:
    // "n1": {
    //     "integerValue": 1
    // },
    // "s1": {
    //     "stringValue": "v1"
    // }
    if(source.has("booleanValue")){
        return source.get("booleanValue");
    }
    else if(source.has("integerValue")){
        return source.get("integerValue");
    }
    else if(source.has("stringValue")){
        return source.get("stringValue");
    }
    else if(source.has("timestampValue")){
        return source.get("timestampValue");
    }
    // If object is a map we will call the method like it was the firestore document
    // Example:
    // "m1": {
    //     "mapValue": {
    //         "fields": {
    //             "s2": {
    //                 "stringValue": "v2"
    //             }
    //         }
    //     }
    // }
    else if(source.has("mapValue")){
        JsonObject jObjectMap = transformFirestoreToJsonObject(source.get("mapValue").getAsJsonObject().get("fields").getAsJsonObject());
        return jObjectMap;
    }
    // If object in an array we will iterate over all elements to get their values
    // Example: 
    // "a1": {
    //     "arrayValue": {
    //         "values": [{
    //                 "stringValue": "aa1"
    //             }, {
    //                 "integerValue": 2
    //             }
    //         ]
    //     }
    // }
    else if(source.has("arrayValue")){
        JsonArray jObjectArray = new JsonArray();
        JsonArray jArray = source.get("arrayValue").getAsJsonObject().get("values").getAsJsonArray();
        for(JsonElement jArrayElement : jArray){
            JsonElement jElement = evaluateJsonObject(jArrayElement.getAsJsonObject());
            jObjectArray.add(jElement);
        }
        return jObjectArray;
    }
    return null;
  }
}