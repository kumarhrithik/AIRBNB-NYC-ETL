# Airbnb NYC ETL Pipeline

This repository contains an ETL pipeline script that processes the Airbnb New York City dataset. The pipeline is implemented using Python, MongoDB, and Metaflow.

## Repository Structure

airbnb-nyc-etl/
├── data/
│ └── your_dataset.csv # Placeholder for your dataset
├── scripts/
│ └── etl_pipeline.py # Your ETL pipeline script
├── README.md # Project instructions and documentation
└── requirements.txt # Required Python libraries


## Setup and Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/your-username/airbnb_etl_pipeline.git
   cd airbnb_etl_pipeline

2. **Install Required Libraries**
     pip install -r requirements.txt

3. **Set Up MongoDB**

Ensure MongoDB is installed and running on your machine. You can download it from here.
Update File Path

Update the file path in the etl_pipeline.py script with the path to your Airbnb dataset.

## Detailed Explanation of the ETL Steps

start:

Sets up the file path and establishes a connection to MongoDB.
create_collections:

Creates the collections listings and avg_price_per_neighbourhood in MongoDB if they do not already exist.
load_data:

Reads the dataset from the specified file path.
Converts the data into a list of dictionaries.
Inserts the data into the MongoDB collection listings.
extract_data:

Extracts data from the listings collection in MongoDB.
Converts the extracted data into a pandas DataFrame.
transform_data:

Fills missing values in the reviews_per_month and last_review columns.
Converts the last_review column to a datetime format.
Calculates the average price per neighborhood and stores it in a new DataFrame.
load_transformed_data:

Drops the existing avg_price_per_neighbourhood collection if it exists.
Inserts the transformed data into the avg_price_per_neighbourhood collection in MongoDB.
end:

Prints a message indicating the successful completion of the ETL process.