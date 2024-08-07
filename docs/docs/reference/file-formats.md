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

## `.yaml`

`.yaml` files are loaded using `yaml.load` and the one record is returned per file.
The record is the entire parsed contents of the `.yaml` file.


# Compressed File Formats

## `.gz`
`{File Format Extension}.gz` files are decompressed using `gzip.open` and stripped of the .gz extension and processed in the subsequent extension.

## `.bz2`
`{File Format Extension}.bz2` files are decompressed using `bz2.open` and stripped of the .bz2 extension and processed in the subsequent extension.