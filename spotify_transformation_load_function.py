import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd


def album(data):
    album_list= []
    #To store the elements from the album elements after every set period of time, we have got this
    # list made in order to collect the dict that we are making after perfoming what is done below

    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_elements = {'album_id': album_id, 'album_name': album_name, 'album_release_date': album_release_date, 
                          'album_total_tracks': album_total_tracks, 'album_url': album_url}   
        
        album_list.append(album_elements)
    return album_list

        
def artist(data):
    #Making a list to hold all the data after performing all the action that is happening under this
    artist_list = []
    
    for row in data['items']:
        for key, value in row.items():                          #The items () method in the dictionary is used to return each item in
                                                                    # a dictionary as tuples in a list.
            if key == 'track':
                for artist in value['artists']:
                                                   #Now making a dict from all this data retrieved
                    artist_dict = {'artist_id': artist['id'], 'artist_name': artist['name'], 'external_url': artist['href']}
                    artist_list.append(artist_dict)
    return artist_list


def song(data):
    song_list= []
    
    for row in data['items']:
        song_id= row['track']['id']
        song_name= row['track']['name']
        song_duration= row['track']['duration_ms']
        song_url= row['track']['external_urls']['spotify']
        song_popularity= row['track']['popularity']
        song_added= row['added_at']
        album_id= row['track']['album']['id']
        song_id= row['track']['id']
        artist_id= row['track']['album']['artists'][0]['id']
    
         #making a dict to assemble these all in one place
        song_element= {'song_id': song_id, 'song_name': song_name, 'duration_ms': song_duration, 'url': song_url,
                      'popularity': song_popularity, 'song_added': song_added, 'album_id': album_id,
                       'artist_id': artist_id}
        song_list.append(song_element)
    return song_list
        

def lambda_handler(event, context):
    s3= boto3.client('s3')
    Bucket = "spotify-elt-project-adarsh"
    Key = "raw_data/to_be_processed/"
    
    #print(s3.list_objects(Bucket = Bucket, Prefix = Key))
    #print(s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents'])      #Prefix ie the key and bucket are available in the dict inside of the 'Contents' of the list
    
    spotify_data = []         #Making a list to put all the data that will be collected from the operations performed below
    spotify_key = []           #Making a list to store the locations of data
                                                               
    for file in s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents']:
        #print(file['Key'])
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':            #So, this way we make sure to only pick a file with .json format from the mentioned postion
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']           #Inside this "response['Body']", we have the actual data
            jsonObject = json.loads(content.read())
            print(jsonObject)
            
            spotify_data.append(jsonObject)
            spotify_key.append(file_key)
        
    for data in spotify_data:    
        album_list = album(data)
        artist_list = artist(data)
        song_list = song(data)
        
        print(album_list)
        
        #Turning all lists into DataFrame format
        
        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset =['album_id'])
        
        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset =['artist_id'])
        
        song_df = pd.DataFrame.from_dict(song_list)
        song_df = song_df.drop_duplicates(subset =['song_id'])
        
        #datetime conversion
        album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'])
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])

        #Now, saving the transformed data into places
        
        #1 SONG DATA
        #song_key is just the name for the filename that I am putting that's it
        song_key = "transformed_data/song_data/" + str(datetime.now()) + ".csv"
        #We have to convert the entire DataFrame in the string format , so below it is
        song_buffer = StringIO()           #Creating an object using StringIO
        song_df.to_csv(song_buffer, index = False)   #We are putting 'index = False' because we don't want glue-crawler to have trouble detecting full file because index side is also created on its own
                                                     #This will convert the entire song DF into String-like object
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = song_key, Body = song_content)
        
        
        #2 ALBUM DATA
        #album_key is just the name for the filename that I am putting that's it
        album_key = "transformed_data/album_data/" + str(datetime.now()) + ".csv"
        #We have to convert the entire DataFrame in the string format , so below it is
        album_buffer = StringIO()           #Creating an object using StringIO
        album_df.to_csv(album_buffer, index = False)        #This will convert the entire album DF into String-like object
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = album_key, Body = album_content)
        
        
        #3 ARTIST DATA
        #artist_key is just the name for the filename that I am putting that's it
        artist_key = "transformed_data/artist_data/" + str(datetime.now()) + ".csv"
        #We have to convert the entire DataFrame in the string format, so below it is
        artist_buffer = StringIO()          #Creating an object using StringIO
        artist_df.to_csv(artist_buffer, index = False)        #This will convert the entire artist DF into Stirng-like object
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = artist_key, Body = artist_buffer.getvalue())
        
    #Now, copying the file from 'to be processed' to 'processed' location, and then deleting the files from the prior location
    s3_resource = boto3.resource('s3')
    for key in spotify_key:
        copy_source ={
            'Bucket' : Bucket,
            'Key' : key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split('/')[-1])    #Using 'key.split('/')[-1]' to get the exact name for the copied file
        s3_resource.Object(Bucket, key).delete()                                                         #To delete the file from the location ie , 'to be processed'
        