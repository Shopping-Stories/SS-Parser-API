from .entry_upload import item_collection, entries_collection, item_regex, ItemInput, create_item
from ..api_types import Message
import pandas as pd
from fastapi import APIRouter
from bson import ObjectId
from boto3 import client

router = APIRouter()

# IN PROGRESS -- needs proper credentials 
# downloads specified file "filename" (including file type) from aws
def download_item_file(filename):
    print('/Items/' + filename)
    s3 = client("s3")
    s3.download_file("shoppingstories", filename, '/Items/' + filename)
    

@router.post("/combine_items/", tags=["Items Management"], response_model=Message)
def combine_items(primary_item: str, secondary_item: str, new_item_name: str):
    """
    Combines two items (both specified by name) in the database. Inherits data from primary item, unless primary item has no data in a field. Case sensitive.
    Sets error flag and has ERROR at the front of the message if any errors occur. People will not be combined if any error occurs.
    """

    primary_item_data = item_collection.find_one({'item': primary_item})
    secondary_item_data = item_collection.find_one({'item': secondary_item})

    if primary_item_data == None:
        return Message(message=f"ERROR: {primary_item} not found.", error=True)
    if secondary_item_data == None:
        return Message(message=f"ERROR: {secondary_item} not found.", error=True)
        
    primary_item_id = primary_item_data['_id'] 
    secondary_item_id = secondary_item_data['_id'] 

    if primary_item_id == secondary_item_id:
        return Message(message=f"ERROR: Both items are the same.", error=True)
    
    item_collection.update_one({'_id': primary_item_id}, {'$set': {'item': new_item_name}})

    if 'related' in primary_item_data and 'related' in secondary_item_data:
        for secondary_related in secondary_item_data['related']:
            if secondary_related != primary_item_id and secondary_related not in primary_item_data['related']:
                item_collection.update_one({'_id': primary_item_id}, {'$push': {'related': secondary_related}})
        if secondary_item_id in primary_item_data['related']:
            item_collection.update_one({'_id': primary_item_id}, {'$pull': {'related': secondary_item_id}})
    elif 'related' in secondary_item_data:
        for secondary_related in secondary_item_data['related']:
            if secondary_related != primary_item_id:
                item_collection.update_one({'_id': primary_item_id}, {'$push': {'related': secondary_related}})

    if 'archMat' not in primary_item_data and 'archMat' in secondary_item_data:
        item_collection.update_one({'_id': primary_item_id}, {'$set': {'archMat': secondary_item_data['archMat']}})
    
    if 'category' not in primary_item_data and 'category' in secondary_item_data:
        item_collection.update_one({'_id': primary_item_id}, {'$set': {'category': secondary_item_data['category']}})

    if 'subcategory' not in primary_item_data and 'subcategory' in secondary_item_data:
        item_collection.update_one({'_id': primary_item_id}, {'$set': {'subcategory': secondary_item_data['subcategory']}})

    item_collection.delete_one({'_id': secondary_item_id})

    # update related items
    item_collection.update_many({'related': secondary_item_id}, {'$push': {'related': primary_item_id}})
    item_collection.update_many({'related': secondary_item_id}, {'$pull': {'related': secondary_item_id}})
    # update entries
    entries_collection.update_many({'itemID': secondary_item_id}, {'$set': {'itemID': primary_item_id, 'item': new_item_name}})

    return Message(message=f"Successfully compined items as {primary_item_id}.")


@router.post("/upload_items/", tags=["Items Management"], response_model=Message)
def item_upload(file_name: str):
    """
    Uploads items from a master list's categories sheet. Categories must be FIRST or ONLY sheet in the document, or function will fail. File must be in \api\ssParser\...
    If categories section is not read, function will fail. If data differs from expected formatting, all data prior to error will still be entered.
    """

    df = pd.read_excel("api\ssParser\\" + file_name)

    for index, row in df.iterrows():
        ## reformats items with commas
        if ", " in row['Item']:
            split_input = row['Item'].split(", ")
            sorted_input = list(reversed(split_input))
            joined_input = " ".join(sorted_input)
            row['Item'] = joined_input

        if item_collection.find_one({'item': {"$regex": "^" + row['Item'] + "$", "$options": 'i'}}):
            
            item_data = item_collection.find_one({'item': {"$regex": "^" + row['Item'] + "$", "$options": 'i'}})
            item_collection.update_one({'_id': item_data['_id']}, {'$set': {'category': row['Category'], 'subcategory': row['Subcategory'], 'archMat': row['ArchMat']}})
            
            if 'related' in item_data:
                for related in item_data['related']:
                    # print("has related")
                    if related is None:
                        continue
                    found = item_collection.find_one({'_id': related})
                    if found is None:
                        continue
                    
                    if 'category' not in item_collection.find_one({'_id': related}):
                        item_collection.update_one({'_id': related}, {'$set': {'category': row['Category'], 'subcategory': row['Subcategory'], 'archMat': row['ArchMat']}})
                    
        else:
            new_item_id = item_collection.insert_one({'item': row['Item'], 'category': row['Category'], 'subcategory': row['Subcategory'], 'archMat': row['ArchMat']}).inserted_id
            relatedItems = item_regex(row['Item'])
            for item in relatedItems:
                if new_item_id != item["_id"]:
                    item_collection.update_one({'_id': item["_id"]}, {'$push': {'related': new_item_id}}) 
                    item_collection.update_one({'_id': new_item_id}, {'$push': {'related': item["_id"]}}) 
                    if 'category' not in item:
                        item_collection.update_one({'_id': item['_id']}, {'$set': {'category': row['Category'], 'subcategory': row['Subcategory'], 'archMat': row['ArchMat']}})

    return Message(message="Successfully uploaded item data.")

@router.post("/add_item_relationship/", tags=["Items Management"], response_model=Message)
def add_item_relationship(item1: str, item2: str):
    """
    Creates a relationship between two items (both specified by name) in the database. Inherits data from primary item.
    Sets error flag and has ERROR at the front of the message if any errors occur. No relationships will be updated if any error occurs.
    """

    item1_data = item_collection.find_one({'item': item1})
    item2_data = item_collection.find_one({'item': item2})

    if item1_data == None:
        return Message(message=f"ERROR: {item1} not found.", error=True)
    if item2_data == None:
        return Message(message=f"ERROR: {item2} not found.", error=True)
        
    item1_id = item1_data['_id'] 
    item2_id = item2_data['_id'] 

    if item1_id != item2_id:
        if "related" in item1_data:
            for related in item1_data['related']:
                if related == item2_id:
                    return Message(message=f"ERROR: Relationship already exists.", error=True)
        if "related" in item2_data:
            for related in item2_data['related']:
                if related == item1_id:
                    return Message(message=f"ERROR: Relationship already exists.", error=True)
        item_collection.update_one({'_id': item1_id}, {'$push': {'related': item2_id}}) 
        item_collection.update_one({'_id': item2_id}, {'$push': {'related': item1_id}})
    else: 
        return Message(message=f"ERROR: Both items are the same.", error=True)

    return Message(message="Successfully added relationship.")


@router.post("/edit_item/", tags=["Items Management"], response_model=Message)
def edit_item(item_id: str, new_values: ItemInput):
    """
    Edits an item (specified by ID) in the database.
    Sets error flag and has ERROR at the front of the message if any errors occur. No data will be edited in an error occurs.
    """

    if item_collection.find_one({'_id': ObjectId(item_id)}) == None:
        return Message(message=f"ERROR: Item {item_id} not found.", error=True)

    new_values = new_values.dict()

    todel = []
    for key in new_values:
        if new_values[key] == None:
            todel.append(key)

    for key in todel:
        del new_values[key]

    item_collection.update_one({'_id': ObjectId(item_id)}, {"$set": new_values})

    # edit item name in entries
    if "item" in new_values:
        entries_collection.update_many({'itemID': ObjectId(item_id)}, {'$set': {'item': new_values["item"]}})

    return Message(message="Successfully edited item.")


@router.post("/delete_item/", tags=["Items Management"], response_model=Message)
def remove_item(item_id: str):
    """
    Removes a specified item from the database (specified by ID).
    Sets error flag and has ERROR at the front of the message if any errors occur.
    """

    if item_collection.find_one({'_id': ObjectId(item_id)}):
        item_collection.delete_one({'_id': ObjectId(item_id)})
        return Message(message="Successfully deleted item.")
    else:
        return Message(message=f"ERROR: Item {item_id} not found.", error=True)

@router.post("/create_item/", tags=["Items Management"], response_model=Message)
def insert_item(item: str, archMat: int = "", category: str = "", subcategory: str = ""): #add all data
    """
    Manually creates a new item from user input. 
    """
    item_id = create_item(item)

    if archMat:
        item_collection.update_one({'_id': ObjectId(item_id)}, {"$set": {"archMat": archMat}}) 
    if category:
        item_collection.update_one({'_id': ObjectId(item_id)}, {"$set": {"category": category}})
    if subcategory:
        item_collection.update_one({'_id': ObjectId(item_id)}, {"$set": {"subcategory": subcategory}})
    
    return Message(message=f"Successfully inserted item. New item has id {item_id}")
