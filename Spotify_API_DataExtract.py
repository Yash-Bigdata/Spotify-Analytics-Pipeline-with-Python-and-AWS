import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    client_id=os.environ.get('client_id')
    client_secret=os.environ.get('client_secret')
    
    spotify_credentials= SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    spotify = spotipy.Spotify(client_credentials_manager=spotify_credentials)
    playlist_link='https://open.spotify.com/playlist/37i9dQZEVXbLZ52XmnySJg'
    playlist_URI=playlist_link.split('/')[-1]
    playlist=spotify.playlist_tracks(playlist_URI)
    cilent =boto3.client('s3')
    
    filename= "spotify_raw_data" + str(datetime.now()) + ".json"
    
    cilent.put_object(
        Bucket="rawdataspotifyetl",
        Key= "Before_Processed/" + filename,
        Body=json.dumps(playlist)
        )
