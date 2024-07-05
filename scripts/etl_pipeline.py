import pandas as pd
from pymongo import MongoClient
from metaflow import FlowSpec, step

class ETLFlow(FlowSpec):

    @step
    def start(self):
        # Initial step to set up file path and MongoDB connection
        self.file_path = 'data\AB_NYC_2019.csv'  # Update with your dataset path
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['airbnb_nyc']
        self.next(self.create_collections)

    @step
    def create_collections(self):
        # Create the listings collection in MongoDB (implicitly created on insert if not exists)
        # Move to the next step
        self.next(self.load_data)

    @step
    def load_data(self):
        # Load dataset
        self.data = pd.read_csv(self.file_path)

        # Convert the DataFrame to a list of dictionaries
        data_dict = self.data.to_dict("records")

        # Insert data into MongoDB
        self.db.listings.insert_many(data_dict)
        self.next(self.transform_data)

    @step
    def transform_data(self):
        # Extract data from MongoDB
        listings = self.db.listings.find()
        self.data = pd.DataFrame(list(listings))

        # Fill missing values
        self.data['reviews_per_month'].fillna(0, inplace=True)
        self.data['last_review'].fillna('1900-01-01', inplace=True)

        # Convert last_review to datetime
        data['last_review'] = pd.to_datetime(data['last_review'], format='%Y-%m-%d')

        # Calculate average price per neighborhood
        self.avg_price_per_neighbourhood = self.data.groupby('neighbourhood')['price'].mean().reset_index()
        self.avg_price_per_neighbourhood.columns = ['neighbourhood', 'avg_price']
        self.next(self.load_transformed_data)

    @step
    def load_transformed_data(self):
        # Drop the existing collection if it exists
        self.db.avg_price_per_neighbourhood.drop()

        # Insert transformed data into MongoDB
        data_dict = self.avg_price_per_neighbourhood.to_dict("records")
        self.db.avg_price_per_neighbourhood.insert_many(data_dict)
        self.next(self.end)

    @step
    def end(self):
        # Final step
        print("ETL process completed successfully!")

if __name__ == '__main__':
    ETLFlow()
