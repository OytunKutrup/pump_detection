from pymongo import MongoClient


class App:

    def __init__(self, mongo_db_username, mongo_db_pass):
        db_uri = "mongodb+srv://" + mongo_db_username + ":" + mongo_db_pass + "@amazoncluster.8donfen.mongodb.net/?retryWrites=true&w=majority"
        self.client = MongoClient(db_uri)

    def close(self):
        self.client.close()

    def get_pumped_coin_data(self):
        crypto_db = self.client["crypto_db"]
        pumped_data_table = crypto_db["pumped_data"]
        data_list = list(pumped_data_table.find().sort({'timestamp': -1}))
        pumped_data = []
        for document in data_list:
            coin_name = document['coinName'].replace("/", "")
            exchange = document['exchange'].upper()
            timestamp = document["timestamp"].strftime("%B %d, %Y at %I:%M %p")
            pumped_data.append([coin_name, exchange, timestamp])
        return pumped_data
