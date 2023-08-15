from flask_restx import Namespace, Resource, marshal_with
from bson import ObjectId
from models import data_model

def register_routes(api):
    data_namespace = api.namespace("data", description="MQTT Data operations")

    @data_namespace.route("/")
    class DataList(Resource):
        @marshal_with(data_model, as_list=True)
        def get(self):
            return list(data_collection.find())

    @data_namespace.route("/<string:data_id>")
    @api.response(404, "Data not found")
    class DataItem(Resource):
        @marshal_with(data_model)
        def get(self, data_id):
            return data_collection.find_one({"_id": ObjectId(data_id)})

        @marshal_with(data_model)
        def put(self, data_id):
            data = api.payload
            data_collection.update_one({"_id": ObjectId(data_id)}, {"$set": data})
            return data

        @api.response(204, "Data deleted")
        def delete(self, data_id):
            data_collection.delete_one({"_id": ObjectId(data_id)})
            return "", 204
