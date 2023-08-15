import pymongo

# MongoDB connection settings
mongodb_host = "localhost"
mongodb_port = 27017
mongodb_database = "mqtt_db"
import pymongo

# MongoDB connection settings
mongodb_host = "localhost"
mongodb_port = 27017
mongodb_database = "mqtt_db"

# Function to get the list of topics and their types from the MongoDB database
def get_topic_list():
    try:
        # Establish a connection to the MongoDB server
        client = pymongo.MongoClient(mongodb_host, mongodb_port)
        db = client[mongodb_database]

        # Get the topic list from the database
        topic_list = []  # Initialize an empty list
        collection = db['data']  # Collection for topics
        for item in collection.find():
            # Extract relevant fields from the document
            topic = item.get('topic')
            topic_type = item.get('type')
            parsed = item.get('parsed')
            
            # Append the topic details to the list
            topic_list.append({
                'topic': topic,
                'type': topic_type,
                'parsed': parsed
            })
        
        # Close the MongoDB connection
        client.close()

        return topic_list
    except Exception as e:
        print(f"Error getting topic list from MongoDB: {str(e)}")
        return []

# Example usage
if __name__ == "__main__":
    topics = get_topic_list()
    if topics:
        for topic in topics:
            print(f"Topic: {topic['topic']}, Type: {topic['type']}, Parsed: {topic['parsed']}")
    else:
        print("No topics found or there was an error.")

