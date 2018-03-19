# landmark-rest README

## (Optional)
Install [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/) to make your python life easier. :blush:

## Install requirements ##
```
pip install -r requirements.txt
```

## Setup the database ##
```
python setup_db.py
```

## Run the app ##
```
python app.py
```

## Code "layout" ##
`clustering.py` - endpoints and methods for clustering

`extraction.py` - endpoints and methods for extraction

`harvesting.py` - endpoints and methods for harvesting

`learning.py` - endpoints and methods for learning

`project.py` - endpoints for the landmark UI

`data/` - encoder, models and views for the system

`util/` - helper functions and classes from previous iterations of the tool




## Use the following curl command to test the learning endpoint ##
```bash
curl -H "Content-Type: application/json" -X POST http://localhost:5000/learning/unsupervised -d\
'{
  "pages": {
      "page1": "test me please - what about this table <table style=\"width:100%\"> <tr> <th>Firstname</th> <th>Lastname</th> <th>Age</th> </tr> <tr> <td>Bill</td> <td>Wilson</td> <td>33</td> </tr> <tr> <td>Bubba</td> <td>Yourd</td> <td>66</td> </tr> </table>",
      "page2": "test you please - what about this table <table style=\"width:100%\"> <tr> <th>Secondname</th> <th>hello</th> <th>Age</th> </tr> <tr> <td>Jill</td> <td>Smith</td> <td>50</td> </tr> <tr> <td>Eve</td> <td>Jackson</td> <td>94</td> </tr> </table>"
  }
}'
```
**Response:**
```json
{
  "template": {
    "markup": {
      "page1": {
        "-whataboutthistable0005": {
          "begin_index": 87, 
          "end_index": 96, 
          "extract": "Firstname", 
          "rule_id": "aa75710a-334d-458e-bcb5-f4298c8ea99b"
        }, 
        "0007": {
          "begin_index": 106, 
          "end_index": 114, 
          "extract": "Lastname", 
          "rule_id": "607d9120-c56d-4c34-966c-3d470037f2ad"
        }, 
        "0012": {
          "begin_index": 201, 
          "end_index": 239, 
          "extract": "Bubba</td> <td>Yourd</td> <td>66</td>", 
          "rule_id": "db221681-c05c-41e5-a22c-f4070e9314ff"
        }, 
        "Age0011": {
          "begin_index": 148, 
          "end_index": 186, 
          "extract": "Bill</td> <td>Wilson</td> <td>33</td>", 
          "rule_id": "07fdbe8d-217b-47ae-b8ba-c2e8d5b43980"
        }, 
        "_list0001": {
          "begin_index": 50, 
          "end_index": 313, 
          "extract": " <table style=\"width:100%\"> <tr> <th>Firstname</th> <th>Lastname</th> <th>Age</th> </tr> <tr> <td>Bill</td> <td>Wilson</td> <td>33</td> </tr> <tr> <td>Bubba</td> <td>Yourd</td> <td>66</td> </tr> </table> ENDOFPAGE", 
          "rule_id": "c06c2aa4-6150-4968-9968-2cb37cb6de13", 
          "sequence": [
            {
              "begin_index": 87, 
              "end_index": 131, 
              "extract": "Firstname</th> <th>Lastname</th> <th>Age</th", 
              "sequence_number": 1
            }, 
            {
              "begin_index": 148, 
              "end_index": 184, 
              "extract": "Bill</td> <td>Wilson</td> <td>33</td", 
              "sequence_number": 2
            }, 
            {
              "begin_index": 201, 
              "end_index": 237, 
              "extract": "Bubba</td> <td>Yourd</td> <td>66</td", 
              "sequence_number": 3
            }
          ]
        }, 
        "us_state0001": {
          "begin_index": 16, 
          "end_index": 20, 
          "extract": "me", 
          "rule_id": "180c5bf4-50ee-42f0-b0f1-6339d290f207"
        }
      }, 
      "page2": {
        "-whataboutthistable0005": {
          "begin_index": 88, 
          "end_index": 98, 
          "extract": "Secondname", 
          "rule_id": "aa75710a-334d-458e-bcb5-f4298c8ea99b"
        }, 
        "0007": {
          "begin_index": 108, 
          "end_index": 113, 
          "extract": "hello", 
          "rule_id": "607d9120-c56d-4c34-966c-3d470037f2ad"
        }, 
        "0012": {
          "begin_index": 199, 
          "end_index": 237, 
          "extract": "Eve</td> <td>Jackson</td> <td>94</td>", 
          "rule_id": "db221681-c05c-41e5-a22c-f4070e9314ff"
        }, 
        "Age0011": {
          "begin_index": 147, 
          "end_index": 184, 
          "extract": "Jill</td> <td>Smith</td> <td>50</td>", 
          "rule_id": "07fdbe8d-217b-47ae-b8ba-c2e8d5b43980"
        }, 
        "_list0001": {
          "begin_index": 51, 
          "end_index": 312, 
          "extract": " <table style=\"width:100%\"> <tr> <th>Secondname</th> <th>hello</th> <th>Age</th> </tr> <tr> <td>Jill</td> <td>Smith</td> <td>50</td> </tr> <tr> <td>Eve</td> <td>Jackson</td> <td>94</td> </tr> </table> ENDOFPAGE", 
          "rule_id": "c06c2aa4-6150-4968-9968-2cb37cb6de13", 
          "sequence": [
            {
              "begin_index": 88, 
              "end_index": 130, 
              "extract": "Secondname</th> <th>hello</th> <th>Age</th", 
              "sequence_number": 1
            }, 
            {
              "begin_index": 147, 
              "end_index": 182, 
              "extract": "Jill</td> <td>Smith</td> <td>50</td", 
              "sequence_number": 2
            }, 
            {
              "begin_index": 199, 
              "end_index": 235, 
              "extract": "Eve</td> <td>Jackson</td> <td>94</td", 
              "sequence_number": 3
            }
          ]
        }, 
        "us_state0001": {
          "begin_index": 16, 
          "end_index": 21, 
          "extract": "you", 
          "rule_id": "180c5bf4-50ee-42f0-b0f1-6339d290f207"
        }
      }
    }, 
    "rules": [
      {
        "begin_regex": "test", 
        "end_regex": "please\\s+\\-\\s+what\\s+about\\s+this\\s+table", 
        "id": "180c5bf4-50ee-42f0-b0f1-6339d290f207", 
        "include_end_regex": true, 
        "name": "us_state0001", 
        "removehtml": false, 
        "rule_type": "ItemRule", 
        "strip_end_regex": "please\\s+\\-\\s+what\\s+about\\s+this\\s+table", 
        "visible_chunk_after": "please - what about this table", 
        "visible_chunk_before": "test"
      }, 
      {
        "begin_regex": ":100%\"\\>\\s+\\<.*?tr\\>\\s+\\<th\\>", 
        "end_regex": "\\</", 
        "id": "aa75710a-334d-458e-bcb5-f4298c8ea99b", 
        "include_end_regex": true, 
        "name": "-whataboutthistable0005", 
        "removehtml": false, 
        "rule_type": "ItemRule", 
        "strip_end_regex": "\\</", 
        "visible_chunk_before": "- what about this table"
      }, 
      {
        "begin_regex": ":100%\"\\>\\s+\\<.*?th\\>\\s+\\<th\\>", 
        "end_regex": "\\</", 
        "id": "607d9120-c56d-4c34-966c-3d470037f2ad", 
        "include_end_regex": true, 
        "name": "0007", 
        "removehtml": false, 
        "rule_type": "ItemRule", 
        "strip_end_regex": "\\</"
      }, 
      {
        "begin_regex": "th\\>\\s+\\<th\\>Age.*?\\</th\\>\\s+\\</.*?tr\\>\\s+\\<tr\\>\\s+\\<.*?td\\>", 
        "end_regex": "\\</tr\\>\\s+\\<tr", 
        "id": "07fdbe8d-217b-47ae-b8ba-c2e8d5b43980", 
        "include_end_regex": true, 
        "name": "Age0011", 
        "removehtml": false, 
        "rule_type": "ItemRule", 
        "strip_end_regex": "\\</tr\\>\\s+\\<tr", 
        "visible_chunk_before": "Age"
      }, 
      {
        "begin_regex": "\\</th\\>\\s+\\</.*?\\</tr\\>\\s+\\<tr.*?\\>\\s+\\<td\\>", 
        "end_regex": "\\</tr\\>\\s+\\</", 
        "id": "db221681-c05c-41e5-a22c-f4070e9314ff", 
        "include_end_regex": true, 
        "name": "0012", 
        "removehtml": false, 
        "rule_type": "ItemRule", 
        "strip_end_regex": "\\</tr\\>\\s+\\</"
      }, 
      {
        "begin_regex": "please\\s+\\-\\s+what\\s+about\\s+this\\s+table", 
        "end_regex": "", 
        "id": "c06c2aa4-6150-4968-9968-2cb37cb6de13", 
        "include_end_regex": true, 
        "iter_begin_regex": "\\>\\s+\\<tr\\>\\s+\\<.*?\\>", 
        "iter_end_regex": "\\>\\s+\\</tr\\>\\s+\\<", 
        "name": "_list0001", 
        "no_first_begin_iter_rule": false, 
        "no_last_end_iter_rule": false, 
        "rule_type": "IterationRule", 
        "strip_end_regex": ""
      }
    ], 
    "stripes": [
      {
        "id": 0, 
        "level": 2, 
        "page_locations": {
          "page1": 0, 
          "page2": 0
        }, 
        "stripe": "BEGINOFPAGEtest", 
        "tuple_size": 2
      }, 
      {
        "id": 1, 
        "level": 1, 
        "page_locations": {
          "page1": 3, 
          "page2": 3
        }, 
        "stripe": "please-whataboutthistable", 
        "tuple_size": 6
      }, 
      {
        "id": 2, 
        "level": 1, 
        "page_locations": {
          "page1": 9, 
          "page2": 9
        }, 
        "stripe": "<tablestyle=\"width", 
        "tuple_size": 6
      }, 
      {
        "id": 3, 
        "level": 1, 
        "page_locations": {
          "page1": 15, 
          "page2": 15
        }, 
        "stripe": ":100%\"><", 
        "tuple_size": 6
      }, 
      {
        "id": 4, 
        "level": 2, 
        "page_locations": {
          "page1": 21, 
          "page2": 21
        }, 
        "stripe": "tr><th>", 
        "tuple_size": 5
      }, 
      {
        "id": 5, 
        "level": 3, 
        "page_locations": {
          "page1": 27, 
          "page2": 27
        }, 
        "stripe": "</", 
        "tuple_size": 2
      }, 
      {
        "id": 6, 
        "level": 2, 
        "page_locations": {
          "page1": 29, 
          "page2": 29
        }, 
        "stripe": "th><th>", 
        "tuple_size": 5
      }, 
      {
        "id": 7, 
        "level": 3, 
        "page_locations": {
          "page1": 35, 
          "page2": 35
        }, 
        "stripe": "</", 
        "tuple_size": 2
      }, 
      {
        "id": 8, 
        "level": 1, 
        "page_locations": {
          "page1": 37, 
          "page2": 37
        }, 
        "stripe": "th><th>Age", 
        "tuple_size": 6
      }, 
      {
        "id": 9, 
        "level": 1, 
        "page_locations": {
          "page1": 43, 
          "page2": 43
        }, 
        "stripe": "</th></", 
        "tuple_size": 6
      }, 
      {
        "id": 10, 
        "level": 3, 
        "page_locations": {
          "page1": 49, 
          "page2": 49
        }, 
        "stripe": "tr><tr><", 
        "tuple_size": 6
      }, 
      {
        "id": 11, 
        "level": 2, 
        "page_locations": {
          "page1": 78, 
          "page2": 78
        }, 
        "stripe": "</tr><tr", 
        "tuple_size": 6
      }, 
      {
        "id": 12, 
        "level": 1, 
        "page_locations": {
          "page1": 109, 
          "page2": 109
        }, 
        "stripe": "</tr></", 
        "tuple_size": 6
      }, 
      {
        "id": 13, 
        "level": 2, 
        "page_locations": {
          "page1": 115, 
          "page2": 115
        }, 
        "stripe": "table>ENDOFPAGE", 
        "tuple_size": 3
      }
    ], 
    "supervised": false
  }
}
```


## Dockerize app ##
```
docker build -t landmark-rest .
```

## instantiate docker container ##
First, modify docker.env to configure the system
```
docker run -t -p 5000:5000 --env-file ./docker.env landmark-rest
```

## start/stop docker container ##
```
docker start/stop [container_id]
```