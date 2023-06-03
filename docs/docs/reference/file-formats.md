# File Formats

## `.json`

`.json` files are loaded using `json.load` and the one record is returned per file.
The record is the entire parsed contents of the `.json` file.

## `.txt`

`.txt` files are read line by line and one record is produced per line. The record is in the shape:

```json
{"line": "{{the line}}"}
```

## `.csv`

A `.csv` file is loading using `csv.DictReader`. For each row of the file, one record will be produced. Assuming you
have a csv file with the format:

```csv
name,age
bob,29
```

You will get a record in the following shape:

```json
{"name": "bob", "age": 29}
```
