#-------------------------------------------------------------------------
# AUTHOR: Jeffrey Ni
# FILENAME: db_connection_mongo.py
# SPECIFICATION: Menu functions
# FOR: CS 4250- Assignment #2
# TIME SPENT: 5hrs
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import pymongo

def connectDataBase():

    # Create a database connection object using pymongo
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    database = client["Menu"]
    collection = database["Docs"]
    return collection

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    words = docText.lower().split()
    term_frequency = {}
    for word in words:
        if word in term_frequency:
            term_frequency[word] += 1
        else:
            term_frequency[word] = 1

    # create a list of dictionaries to include term objects.
    term_list = []  # Initialize an empty list for the terms
    for word, count in term_frequency.items():  # Iterate over the dictionary to build the term objects
        term_obj = {}  # Create a dictionary for each term
        term_obj["term"] = word  # Set the "term" key
        term_obj["count"] = count  # Set the "count" key
        term_list.append(term_obj)  # Append the dictionary to the list

    #Producing a final document as a dictionary including all the required document fields
    document = {
        "_id": docId,
        "text": docText,
        "title": docTitle,
        "date": docDate,
        "category": docCat,
        "terms": term_list
    }

    # Insert the document
    col.insert_one(document)

def deleteDocument(col, docId):

    # Delete the document from the database
    col.delete_one({"_id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    col.delete_one({"_id": docId})

    # Create a new document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    inverted_index = {}
    docs = col.find()

    for doc in docs:
        for term_info in doc["terms"]:
            term = term_info["term"]
            count = term_info["count"]
            if term not in inverted_index:
                inverted_index[term] = []  # Initialize an empty list if the term is not already present
            inverted_index[term].append(f"{doc['title']}:{count}")  # Append the title and count
    
    return inverted_index
