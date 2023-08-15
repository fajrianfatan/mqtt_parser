from flask import Flask, request
from flask_restx import Api, Resource, fields
from pymongo import MongoClient
from bson import ObjectId

app = Flask(__name__)
api = Api(app, version="1.0", title="MQTT Data API")

# Create a MongoDB client
client = MongoClient("localhost", 27017)
db = client["mqtt_db"]
data_collection = db["data"]

data_model = api.model("Data", {
    "_id": fields.String(readonly=True, description="The data identifier"),
    "topic": fields.String(required=True, description="The topic"),
    "type": fields.String(required=True, description="The type"),
    "parsed": fields.Boolean(description="Whether data is parsed"),
})

@api.route("/data")
class DataList(Resource):
    @api.marshal_list_with(data_model)
    def get(self):
        return list(data_collection.find())

    @api.expect(data_model)
    @api.marshal_with(data_model, code=201)  # Use 201 status code for successful creation
    def post(self):
        new_data = api.payload
        inserted_data = data_collection.insert_one(new_data)
        new_data["_id"] = str(inserted_data.inserted_id)
        return new_data

@api.route("/data/<string:data_id>")
@api.response(404, "Data not found")
class DataItem(Resource):
    @api.marshal_with(data_model)
    def get(self, data_id):
        return data_collection.find_one({"_id": ObjectId(data_id)})

    @api.expect(data_model)
    @api.marshal_with(data_model)
    def put(self, data_id):
        updated_data = api.payload
        data_collection.update_one({"_id": ObjectId(data_id)}, {"$set": updated_data})
        return updated_data

    @api.response(204, "Data deleted")
    def delete(self, data_id):
        data_collection.delete_one({"_id": ObjectId(data_id)})
        return "", 204

if __name__ == "__main__":
    app.run(debug=True)
