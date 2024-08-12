import google.generativeai as genai
from tqdm import tqdm
import time
import json

#add key gemini here
key = ''
genai.configure(api_key=key)
model = genai.GenerativeModel('gemini-1.5-flash')

safe = [
    {
        "category": "HARM_CATEGORY_HARASSMENT",
        "threshold": "BLOCK_NONE",
    },
    {
        "category": "HARM_CATEGORY_HATE_SPEECH",
        "threshold": "BLOCK_NONE",
    },
    {
        "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
        "threshold": "BLOCK_NONE",
    },
    {
        "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
        "threshold": "BLOCK_NONE",
    },
]


PROMPT = """
You will be given descriptions of books provided by users. 
Your task is to extract specific pieces of information from each description and organize them into a dictionary. 
The fields to be extracted are: title, author, publisher, year, isbn, unknown. 

If any information is missing from the user's description, you should return null for that field, or if you cant define any information from description, you should add it to unknown field. 
The output should be structured as a json, with each field represented as a key and the extracted or missing information as the value.

Example:
User Description: "I'm looking for a fantasy book titled 'The Enchanted Forest' by John Smith, published in 2015 by Magic Press, 300 pages, in English. and have isbn 0060973129"

Output:
{
  "title": "The Enchanted Forest",
  "author": "John Smith",
  "publisher": "Magic Press",
  "year": 2015,
  "isbn": "0060973129",
  "unknown": Null,
}


User Description: "what"

Output:
{
  "title": Null,
  "author": Null,
  "publisher": Null,
  "year": Null,
  "isbn": Null,
  "unknown": "what",
}

Your goal is to accurately fill out this dictionary based on the information provided in the user's description. 
If a user doesn’t mention the book's isbn, for example, you should return null the 'isbn' field.

The following is the user's description:
```
[USER_DESCRIPTION]
```
"""

def extract_fields(user_input):
    prompt = PROMPT.replace("[USER_DESCRIPTION]", user_input)
    out = model.generate_content(prompt, safety_settings=safe).text.replace("```json", "").replace("```", "").strip()
    return json.loads(out)


if __name__ == "__main__":
    user_input = "Mark Salzman"
    fields = extract_fields(user_input)
    print(fields)
