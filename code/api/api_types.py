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


class TobaccoMark(BaseModel):
    mark_number: Optional[str]
    mark_text: Optional[str]

class TobaccoEntry(BaseModel):
    number: Optional[str]
    gross_weight: Optional[str]
    tare_weight: Optional[str]
    weight: Optional[str]

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
    folio_reference: Optional[str]
    store: Optional[str]
    Marginalia: Optional[str]
    currency_type: Optional[str]
    currency_totaling_contextless: Optional[bool]
    commodity_totaling_contextless: Optional[bool]
    account_name: Optional[str]
    store_owner: Optional[str]
    date_year: Optional[str]
    month: Optional[str]
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
    entry_id: Optional[str]
    tobacco_marks: Optional[List[TobaccoMark]]
    tobacco_location: Optional[str]
    tobacco_amount_off: Optional[str]
    tobacco_entries: Optional[List[TobaccoEntry]]
    currency_colony: Optional[str]
    text_as_parsed: Optional[str]
    original_entry: Optional[str]

# Definition of what parser returns
class ParserOutput(BaseModel):
    errors: Optional[List[str]]
    error_context: Optional[List[List[Union[str, List[str]]]]]
    amount: Optional[str]
    amount_is_combo: Optional[bool]
    item: Optional[str]
    price: Optional[str]
    price_is_combo: Optional[bool]
    phrases: Optional[List[Dict[str, Union[str, List[str]]]]]
    date: Optional[str]
    pounds: Optional[int]
    pounds_ster: Optional[int]
    shillings: Optional[int]
    shillings_ster: Optional[int]
    pennies_ster: Optional[int]
    pennies: Optional[int]
    farthings_ster: Optional[int]
    Marginalia: Optional[str]
    farthings: Optional[int]
    store: Optional[str]
    currency_type: Optional[str]
    currency_totaling_contextless: Optional[bool]
    commodity_totaling_contextless: Optional[bool]
    account_name: Optional[str]
    reel: Optional[int]
    store_owner: Optional[str]
    folio_year: Optional[str]
    folio_page: Optional[int]
    folio_reference: Optional[str]
    entry_id: Optional[str]
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
    text_as_parsed: Optional[str]
    original_entry: Optional[str]
    currency_colony: Optional[str]
    tobacco_marks: Optional[List[TobaccoMark]]
    tobacco_location: Optional[str]
    tobacco_entries: Optional[List[TobaccoEntry]]
    tobacco_amount_off: Optional[str]

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

