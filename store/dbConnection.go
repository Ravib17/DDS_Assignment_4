package store

import (
    "context"
    "log"
    "fmt"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/bson/primitive"
     "encoding/json"

)



// func main() {
//     clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
//     client, err := mongo.Connect(context.TODO(), clientOptions)

//     if err != nil {
//         log.Fatal(err)
//     }

//     err = client.Ping(context.TODO(), nil)

//     if err != nil {
//         log.Fatal(err)
//     }

//     fmt.Println("Connected to MongoDB!")

// }
type dbObject struct {
    client *mongo.Client

} 

func newdbConeection(dbport string) *dbObject {
    fmt.Print("here \n")
    fmt.Print(dbport)
     clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:"+dbport)
     
     client, err := mongo.Connect(context.TODO(), clientOptions)
     if err != nil {
        log.Fatal(err)
        fmt.Print("\n FAILED \n")
        return nil
    }
    fmt.Print("\n Success \n")
     
     
    return &dbObject{
        client: client, 
    }
}

func (dbo *dbObject) createCollection(dbname string, collectionName string) *mongo.Collection {
     collection:= dbo.client.Database("project").Collection("Finalnew")

     return collection
} 

func (dbo *dbObject) insertData(data map[string]interface{},collection *mongo.Collection) string {
    fmt.Print(data)
    fmt.Print("\n\n\n\n adsfssfdf \n\n]n")
    result, err := collection.InsertOne(context.TODO(),bson.M(data))
     if err != nil {
        log.Fatal(err)
        fmt.Print("\n\n\n\n ERRoR \n\n]n")
     }
      fmt.Print(result)
      objectID := result.InsertedID.(primitive.ObjectID)
      
      return objectID.Hex()

}

func (dbo *dbObject) getData(key string,collection *mongo.Collection) []map[string]interface{} {
    
     cursor,err:= collection.Find(context.TODO(), bson.M{"name": key})
     if err != nil {
        log.Fatal(err)
        fmt.Print("\n\n\n\n ERRoR \n\n]n")
     }
     
     fmt.Println(cursor)

     var jsonDocuments []map[string]interface{}


     var bsonDocument bson.D
     var jsonDocument map[string]interface{}
     var temporaryBytes []byte

     for cursor.Next(context.Background()) {
     err = cursor.Decode(&bsonDocument)


    temporaryBytes, err = bson.MarshalExtJSON(bsonDocument, true, true)


    err = json.Unmarshal(temporaryBytes, &jsonDocument)
    if err != nil {
        log.Fatal(err)
        fmt.Print("\n\n\n\n ERRoR \n\n]n")
     }

    jsonDocuments = append(jsonDocuments, jsonDocument)
}

    fmt.Println(jsonDocuments)
     return jsonDocuments

}

func (dbo *dbObject) deleteData(data map[string]interface{},collection *mongo.Collection) {
    fmt.Print(data)
    delete(data,"op")
    fmt.Print(data)
    result, err := collection.DeleteMany(context.TODO(),bson.M{"name":data["name"] , "address.city": data["city"]})
     if err != nil {
        log.Fatal(err)
        fmt.Print("\n\n\n\n ERRoR \n\n]n")
     }
      fmt.Print(result)

}
