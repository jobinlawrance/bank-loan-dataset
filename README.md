# GO scripts to clean and ingest csv into Clickhouse

### TLDR;

```bash
docker compose up -d
sudo apt install csvkit (brew install csvkit)
go run csv-cleaner.go
csvjson output.csv > output.json
go run insert-into-ch.go
```

## Prerequisite
- Docker
- Go
- Python3

## Datasource

The csv file was downloaded from this Kagle dataset [here](https://www.kaggle.com/datasets/zaurbegiev/my-dataset/data?select=credit_train.csv)

## Cleaning the CSV file

Checking the validity of the csv using a cli tool like `csvlint` shows that the 13th column has some inconsistency and a bunch of blank rows

`csvlint credit_train.csv`

<img width="541" alt="image" src="https://github.com/user-attachments/assets/764b9ba0-2eed-4eef-8099-15aad7e17399" />

Apart from that we also notice values marked as **NA**, which idealy should be `NULL` or blank.

The `csv-cleaner.go` script takes care of that and gives us a clean csv called `output.csv`

Running the go script like `go run csv-cleaner.go` and then running `csvlint output.csv` gives us a valid csv 

<img width="369" alt="image" src="https://github.com/user-attachments/assets/8111705e-ca0a-4596-a376-299640eabfab" />

Additionaly I converted the csv to json for further processing (but can be avoided) using `csvjson output.csv > output.json`

## Ingesting the data into Clickhouse

First we start the clickhouse docker instance using `docker compose up -d`

Then running `go run insert-into-ch.go` will first analyze the json file, figure out the column and data type and creates a table called `loan_data`

<img width="443" alt="image" src="https://github.com/user-attachments/assets/4f8539d1-33d6-426e-ab8b-72051f877328" />  

---

If the script is executed successfully we can query clickhouse to confirm

<img width="1142" alt="image" src="https://github.com/user-attachments/assets/53ace5ac-8664-42a2-9a8c-b26183294613" />

