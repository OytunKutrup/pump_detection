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
        coin_exchange_list = []
        for document in data_list:
            coin_name = document['coinName'].replace("/", "")
            exchange = document['exchange'].upper()
            timestamp = document["timestamp"].strftime("%B %d, %Y at %I:%M %p")
            coin_exchange_list.append([coin_name, exchange, timestamp])
        return coin_exchange_list

    @staticmethod
    def _recommend_gkdw(tx, movie_name):
        movie_name = "(?i)" + movie_name
        query = (
            """MATCH (c1:Movie)-[:IN_GENRE]->(g:Genre)<-[:IN_GENRE]-(c2:Movie) MATCH (c1:Movie)-[:HAS]->(
            k:Keyword)<-[:HAS]-(c2:Movie) MATCH (c1:Movie)-[:DIRECTED_BY]->(d:Person)<-[:DIRECTED_BY]-(c2:Movie) 
            MATCH (c1:Movie)-[:WROTE_BY]->(w:Person)<-[:WROTE_BY]-(c2:Movie) WHERE c1 <> c2 AND c1.title =~ 
            $movie_name WITH c1, c2, COUNT(DISTINCT g)*2 + COUNT(DISTINCT k) + COUNT(DISTINCT d) + COUNT(DISTINCT w) 
            as intersection_count 

            WITH c1, c2, intersection_count
            ORDER BY intersection_count DESC, c2.average_rating DESC
            WITH c1, COLLECT([c2, intersection_count])[0..5] as neighbors
            return neighbors
                                """
        )
        result = tx.run(query, movie_name=movie_name)
        return [row for row in result]


if __name__ == "__main__":
    # login_uri = config.NEO4J_URI
    login_user = "neo4j"
    # login_password = config.NEO4J_API_KEY
    # app = App(login_uri, login_user, login_password)
    # app.close()
