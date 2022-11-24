import pymongo
from pathlib import Path
from json import dump, dumps, loads
from dotenv import load_dotenv
from os import environ
from os.path import join, dirname

load_dotenv(join(dirname(__file__), ".env"))

# Function to connect to database
def get_database():  
    # Creates a connection to MongoDB
    client = pymongo.MongoClient(environ.get("MONGO_LOGIN_STRING"))

    # Returns the database collection we'll be using
    return client["shoppingStories"]
 

db = get_database()   # Creates a varibale for the database