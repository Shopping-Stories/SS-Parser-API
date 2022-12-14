from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Union

class Message(BaseModel):
    message: str
    error: bool = False

class ParserProgress(BaseModel):
    progress: float
    filenames: List[str]

class Currency(BaseModel):
    pounds: int
    shillings: int
    pennies: int
    farthings: int = 0

class Ledger(BaseModel):
    reel: str
    folio_year: str
    folio_page: str
    entry_id: str

class DatabaseEntry(BaseModel):
    amount: Optional[str] = "1"
    amount_is_combo: Optional[bool]
    item: Optional[str]
    price: Optional[str]
    price_is_combo: Optional[bool]
    phrases: Optional[List[Dict[str, Union[str, List[str]]]]]
    date: Optional[str]
    currency: Optional[Currency]
    sterling: Optional[Currency]
    ledger: Optional[Ledger]
    Marginalia: Optional[str]
    currency_type: Optional[str]
    currency_totaling_contextless: Optional[bool]
    commodity_totaling_contextless: Optional[bool]
    account_name: Optional[str]
    store_owner: Optional[str]
    date_year: Optional[str] = Field(alias="Date Year")
    month: Optional[str] = Field(alias="_Month")
    Day: Optional[str]
    debit_or_credit: Optional[str]
    context: Optional[List[List[str]]]
    Quantity: Optional[str]
    Commodity: Optional[str]
    people: Optional[List[str]]
    type: Optional[str]
    liber_book: Optional[str]
    mentions: Optional[List[str]]
    itemID: Optional[str]
    peopleID: Optional[str]
    people: Optional[List[str]]
    accountHolderID: Optional[str]
    entryID: Optional[str] = Field(alias="_id")

class EntryList(BaseModel):
    entries: List[DatabaseEntry]

class StringList(BaseModel):
    strings: List[str]


class IncomingFile(BaseModel):
    file: str
    name: str

class IncomingFiles(BaseModel):
    files: List[IncomingFile]

class IncomingFileUrls(BaseModel):
    urls: List[str]

class FailedFile(BaseModel):
    name: str
    reason: str

class FailedFiles(BaseModel):
    files: List[FailedFile]