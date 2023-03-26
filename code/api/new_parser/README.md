# Shopping Stories Parser  
I know the directory structure is a little complex, but the parser was originally a part of the API.  
Once we realized that would be too expensive, we moved the parser onto AWS ECS to reduce costs significantly.  
Luckily, the parser was designed to be mostly separate from the API so this was simple enough.
Essentially nothing in this folder should be referenced by the rest of the API, however things in this folder may reference the rest of the API.  

To run the parser on everything in s3 you can run 
```
python api_entry.py parser
```  
To run on things locally, uncomment the endpoints in parser_endpoints.py and use those.  
___DO NOT___ push to github with those endpoints uncommented or everything will break.