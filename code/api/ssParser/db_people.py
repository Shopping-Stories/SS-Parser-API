from .entry_upload import people_collection, entries_collection, PeopleInput, create_people
from ..api_types import Message
from fastapi import APIRouter
from bson import ObjectId

router = APIRouter()


@router.post("/edit_person/", tags=["People Management"], response_model=Message)
def edit_person(person_id: str, new_values: PeopleInput):
    """
    Edits a person (specified by ID) in the database.
    Sets error flag and has ERROR at the front of the message if any errors occur. No data will be edited in an error occurs.
    """

    if people_collection.find_one({'_id': ObjectId(person_id)}) == None:
        return Message(message=f"ERROR: Person {person_id} not found.", error=True)

    new_values = new_values.dict()

    todel = []
    for key in new_values:
        if new_values[key] == None:
            todel.append(key)

    for key in todel:
        del new_values[key]

    old_name = people_collection.find_one({'_id': ObjectId(person_id)})["name"]
    people_collection.update_one({'_id': ObjectId(person_id)}, {"$set": new_values})

    # edit people/accountHolder containing person in entries
    if "name" in new_values:
        entries_collection.update_many({'accountHolderID': ObjectId(person_id)}, {'$set': {'account_name': new_values["name"]}})
        entries_collection.update_many({'peopleID': ObjectId(person_id)}, {'$push': {'people': new_values["name"]}})
        entries_collection.update_many({'peopleID': ObjectId(person_id)}, {'$pull': {'people': old_name}})

    return Message(message="Successfully edited person.")

@router.post("/add_people_relationship/", tags=["People Management"], response_model=Message)
def add_people_relationship(person1_name: str, person2_name: str):
    """
    Creates a relationship between two people (both specified by name) in the database.
    Sets error flag and has ERROR at the front of the message if any errors occur. No relationships will be updated if any error occurs.
    """

    person1_data = people_collection.find_one({'name': person1_name})
    person2_data = people_collection.find_one({'name': person2_name})

    if person1_data == None:
        return Message(message=f"ERROR: {person1_name} not found.", error=True)
    if person2_data == None:
        return Message(message=f"ERROR: {person2_name} not found.", error=True)
        
    person1_id = person1_data['_id'] 
    person2_id = person2_data['_id'] 

    if person1_id != person2_id:
        if "related" in person1_data:
            for related in person1_data['related']:
                if related == person2_id:
                    return Message(message=f"ERROR: Relationship already exists.", error=True)
        if "related" in person2_data:
            for related in person2_data['related']:
                if related == person1_id:
                    return Message(message=f"ERROR: Relationship already exists.", error=True)
        people_collection.update_one({'_id': person1_id}, {'$push': {'related': person2_id}}) 
        people_collection.update_one({'_id': person2_id}, {'$push': {'related': person1_id}})
    else: 
        return Message(message=f"ERROR: Both people are the same.", error=True)

    return Message(message="Successfully added relationship.")

@router.post("/combine_people/", tags=["People Management"], response_model=Message)
def combine_people(person1_name: str, person2_name: str, new_name: str):
    """
    Combines two people (both specified by name) in the database. Case sensitive.
    Sets error flag and has ERROR at the front of the message if any errors occur. People will not be combined if any error occurs.
    """
    
    person1_data = people_collection.find_one({'name': person1_name})
    person2_data = people_collection.find_one({'name': person2_name})

    if person1_data == None:
        return Message(message=f"ERROR: {person1_name} not found.", error=True)
    if person2_data == None:
        return Message(message=f"ERROR: {person2_name} not found.", error=True)
        
    person1_id = person1_data['_id']
    person2_id = person2_data['_id']

    if person1_id == person2_id:
        return Message(message=f"ERROR: Both people are the same.", error=True)
    
    new_person_id = people_collection.insert_one({"name": new_name}).inserted_id

    new_rel = []
    if "related" in person1_data:
        for related in person1_data['related']:
            if related != person2_id:
                new_rel.append(related)
    if "related" in person2_data:
        for related in person2_data['related']:
            if related != person1_id:
                new_rel.append(related)
    
    if len(new_rel) > 0:
        new_rel_set = [*set(new_rel)]
        for rel in new_rel_set:
            people_collection.update_one({'_id': new_person_id}, {'$push': {'related': rel}})

    people_collection.delete_one({'_id': person1_id})
    people_collection.delete_one({'_id': person2_id})  

    # edit people/accountHolder containing person in entries
    entries_collection.update_many({'accountHolderID': person1_id}, {'$set': {'accountHolderID': new_person_id, 'account_name': new_name}})
    entries_collection.update_many({'accountHolderID': person2_id}, {'$set': {'accountHolderID': new_person_id, 'account_name': new_name}})

    entries_collection.update_many({"$or": [{'peopleID': person1_id}, {'peopleID': person2_id}]}, {'$push': {'peopleID': new_person_id}})
    entries_collection.update_many({'peopleID': new_person_id}, {'$pull': {'peopleID': {"$in": [person1_id, person2_id]}, 'people': {"$in": [person1_name, person2_name]}}})
    entries_collection.update_many({'peopleID': new_person_id}, {'$push': {'people': new_name}})
  
    # update related people in people
    people_collection.update_many({'related': person1_id}, {'$push': {'related': new_person_id}})
    people_collection.update_many({'related': person1_id}, {'$pull': {'related': person1_id}})
    people_collection.update_many({'related': person2_id}, {'$push': {'related': new_person_id}})
    people_collection.update_many({'related': person2_id}, {'$pull': {'related': person2_id}})
    
    return Message(message=f"Successfully compined people as {new_person_id}.")


@router.post("/delete_person/", tags=["People Management"], response_model=Message)
def remove_person(person_id: str):
    """
    Removes a specified person from the database (specified by ID).
    Sets error flag and has ERROR at the front of the message if any errors occur.
    """

    if people_collection.find_one({'_id': ObjectId(person_id)}):
        people_collection.delete_one({'_id': ObjectId(person_id)})
        return Message(message="Successfully deleted person.")
    else:
        return Message(message=f"ERROR: Person {person_id} not found.", error=True)


@router.post("/create_person/", tags=["People Management"], response_model=Message)
def insert_person(person: str): 
    """
    Manually creates a new person from user input. 
    """
    person_id = create_people(person)

    # add additional optional data here if needed
    
    return Message(message=f"Successfully inserted person. New person has id {person_id}")
