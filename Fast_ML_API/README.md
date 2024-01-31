## Stack
- Fastapi
- MongoDB

## Project Structure
```bash
├── Dockerfile
├── README.md
├── data                    // Folder containing .parquet files
│   └── models              // Folder containing .pkl file
├── docker-compose.yml
├── requirements.txt
├── src
│   ├── __pycache__
│   ├── app
│   ├── create-database.py
│   ├── server.py
│   └── utils
└── test_perf
    ├── test_perf.ipynb
    └── test_perf.py        // Script to run performance testing
```

## Data & Model
Before you do anything you need to
1. Move all `.parquet` (request.parquet, user.parquet, restaurant.parquet, ...) files into `data` folder.
2. Movel `model.pkl` file into `data/models` folder.
\
Then `data` directory should be like this.

```bash
├── data
│   ├── models
│   │   └── model.pkl
│   ├── request.parquet
│   ├── restaurant.parquet
│   ├── user.parquet
│   └── user.small.parquet
```

## Setup Deployment

Deploy this project using docker by running

```bash
docker-compose up -d
```

Running this command should see `lmwn-fast-api` and `lmwn-mongo` container.
```bash
docker ps
```

You will need to create restaurant and user collection including insert the data into the database. By runnning this script

```bash
python src/create-database.py
```

## Test the performance

```bash
python test_perf/test_perf.py
```

## Swagger API doc
Go to ```http://localhost:80/docs``` to see api docs and test your custom request.