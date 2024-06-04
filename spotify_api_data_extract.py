import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3                   #boto3 is a package by aws to programmatically communicate aws services
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id =client_id ,client_secret =client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    playlists= sp.user_playlists('spotify')           #I don't understand where this line is coming from
    
    playlist_link ="https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"
    playlist_url = playlist_link.split('/')[-1]
    spotify_data = sp.playlist_tracks(playlist_url)
    #print(spotify_data)
    
    #here, we want to dump the entire json into s3
    client = boto3.client('s3')
    
    #filename will be json dumped in the below section in the body for name purposes
    filename = 'spotify_raw_'+ str(datetime.now()) + '.json'
    
    client.put_object(                                                     #this object method takes three paramenters
        Bucket="spotify-elt-project-adarsh" , 
        Key="raw_data/to_be_processed/" +filename ,
        Body= json.dumps(spotify_data)
        )