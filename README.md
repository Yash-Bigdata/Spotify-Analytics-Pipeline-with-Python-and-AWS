Spotify End-to-End Data Pipeline Project (AWS, Python)

Technologies Used: Python, AWS S3, AWS Lambda, AWS Glue, AWS Athena, AWS IAM

1. Created a Spotify Developer Account to obtain the Client ID and Client Secret key for data extraction.
2. Used Jupyter Notebook locally to install required libraries and access the TOP 50 India Spotify Playlist.
3. Extracted data about albums, artists, and songs using the spotify.playlist_tracks method and store it in lists.
4. Converted the extracted lists to DataFrames and adjust datetime columns.
5. Created two AWS S3 buckets: Raw_Data and Transformed_Data, with specific folders for each data type.
6. Write an AWS Lambda function for data extraction, using environment variables for sensitive values.
7. Uploaded the Spotipy module to AWS Lambda Layer and add it to the function.
8. Used the Lambda function to dump data into the S3 bucket using the boto3 module.
9. Created another Lambda function for data transformation, processing data from Raw_Data to Transformed_Data.
10. Moved the processed files from Raw_Data/Before_Processed to Raw_Data/Processed after transformation.
11. Set up triggers for the Extract Lambda function to run at specified intervals using EventBridge.
12. Set up a trigger for the Transform Lambda function to run when a new object is created in S3.
13. Created AWS Glue crawlers to infer the schema of CSV files in Transformed_Data and create tables.
14. Configured and run crawlers for Songs, Albums, and Artists data.
15. Used AWS Athena to query the data stored in AWS Glue tables by selecting the respective database and tables. 
