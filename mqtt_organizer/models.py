from flask_restx import Namespace, fields

api = Namespace("mqtt", description="MQTT Data")

data_model = api.model("Data", {
    "_id": fields.String(readonly=True, description="The data identifier"),
    "topic": fields.String(required=True, description="The topic"),
    "type": fields.String(required=True, description="The type"),
    "parsed": fields.Boolean(description="Whether data is parsed"),
})
