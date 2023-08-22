import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime


def lambda_handler(event, context):
    
    #Fetch client ID and secret ID from environment variables
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
   
    # Initialize Spotify client with client credentials
    client_credentials_manager = SpotifyClientCredentials(client_id = client_id, client_secret = client_secret )
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlists = sp.user_playlists('spotify')
    
    # Define the link to the playlist
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    
    # Fetch track data from the playlist
    spotify_data = sp.playlist_tracks(playlist_URI)
    
    # Initialize S3 client
    cilent = boto3.client('s3')
    
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    
    # Upload the Spotify data to the 'to-processed' folder in S3 bucket
    cilent.put_object(
        Bucket="spotify-data-etl-pipeline",
        Key="raw_data/to_processed/" + filename,
        Body=json.dumps(spotify_data)
    )   