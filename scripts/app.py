from flask import Flask, request, jsonify
from pymongo import MongoClient
from bson import ObjectId
from flask_restplus import Api, Resource, fields
from flasgger import Swagger

app = Flask(__name__)

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['mqtt_db']

# Create the "data" collection with unique index on "topic"
db.data.create_index("topic", unique=True)

# Initialize Flask-RESTPlus API
api = Api(
    app,
    title='My API',
    version='1.0',
    description='A simple API'
)

# Initialize Swagger
swagger = Swagger(app)

# Define the data model using Flask-RESTPlus fields
data_model = api.model('Data', {
    'topic': fields.String(description='Topic of the data'),
    'type': fields.String(description='Type of the data'),
    'parsed': fields.Boolean(description='Parsed status')
})

# Route to add data (with "topic," "type," and "parsed" attributes) to MongoDB
@api.route('/add_data', methods=['POST'])
class AddData(Resource):
    @api.expect(data_model, validate=True)
    @api.doc(responses={201: 'Created', 400: 'Validation Error'})
    def post(self):
        """Add new data"""
        try:
            data = api.payload
            new_data = {
                "topic": data.get("topic"),
                "type": data.get("type"),
                "parsed": data.get("parsed")
            }
            db.data.insert_one(new_data)
            return {'message': 'Data added successfully'}, 201
        except Exception as e:
            return {'message': str(e)}, 500

# Route to get all data (with "_id" field removed) from MongoDB
@api.route('/get_data', methods=['GET'])
class GetData(Resource):
    @api.doc(responses={200: 'Success'})
    def get(self):
        """Get all data"""
        try:
            data = []
            for item in db.data.find():
                # Remove the "_id" field
                item.pop("_id", None)
                data.append(item)
            return data, 200
        except Exception as e:
            return {'message': str(e)}, 500

# Route to update "topic," "type," and "parsed" attributes of data by ID
@api.route('/update_data/<string:data_id>', methods=['PUT'])
class UpdateData(Resource):
    @api.expect(data_model, validate=True)
    @api.doc(responses={200: 'Success', 404: 'Data not found'})
    def put(self, data_id):
        """Update data by ID"""
        try:
            new_data = api.payload
            updated_data = {
                "topic": new_data.get("topic"),
                "type": new_data.get("type"),
                "parsed": new_data.get("parsed")
            }
            result = db.data.update_one({"_id": ObjectId(data_id)}, {"$set": updated_data})
            if result.matched_count == 1:
                return {'message': 'Data updated successfully'}, 200
            else:
                return {'message': 'Data not found'}, 404
        except Exception as e:
            return {'message': str(e)}, 500

# Route to delete data by "id"
@api.route('/delete_data/<string:data_id>', methods=['DELETE'])
class DeleteData(Resource):
    @api.doc(responses={200: 'Success', 404: 'Data not found'})
    def delete(self, data_id):
        """Delete data by ID"""
        try:
            result = db.data.delete_one({"_id": ObjectId(data_id)})
            if result.deleted_count == 1:
                return {'message': 'Data deleted successfully'}, 200
            else:
                return {'message': 'Data not found'}, 404
        except Exception as e:
            return {'message': str(e)}, 500


# Run the app
if __name__ == '__main__':
    app.run(debug=True)
