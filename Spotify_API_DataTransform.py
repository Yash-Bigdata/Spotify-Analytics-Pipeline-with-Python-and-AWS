import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

def albums(data):
    Albums_List=[]
    for item in data['items']:
        Album_ID=item['track']['album']['id']
        Album_Name=item['track']['album']['name']
        Album_URL=item['track']['album']['external_urls']['spotify']
        Album_total_tracks=item['track']['album']['total_tracks']
        Album_Release_Date=item['track']['album']['release_date']
        Albums_Dictionary_Element={'Album_ID':Album_ID,'Album_Name':Album_Name,'Album_URL':Album_URL,'Album_total_tracks':Album_total_tracks,'Album_Release_Date':Album_Release_Date}
        Albums_List.append(Albums_Dictionary_Element)
    return Albums_List
    
def artists(data):
    Artists_list = []
    for item in data['items']:
        for key, value in item.items():
            if key == "track":
                for artist in value['artists']:
                    artist_dict = {'artist_id':artist['id'], 'artist_name':artist['name'], 'external_url': artist['href']}
                    Artists_list.append(artist_dict)
    return Artists_list
    
def songs(data):
    Songs_list = []
    for item in data['items']:
        song_id = item['track']['id']
        song_name = item['track']['name']
        song_duration = item['track']['duration_ms']
        song_url = item['track']['external_urls']['spotify']
        song_popularity = item['track']['popularity']
        song_added = item['added_at']
        album_id = item['track']['album']['id']
        artist_id = item['track']['album']['artists'][0]['id']
        Songs_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                        'popularity':song_popularity,'song_added':song_added,'album_id':album_id,
                        'artist_id':artist_id
                       }
        Songs_list.append(Songs_element)
    return Songs_list

def lambda_handler(event, context):
    s3=boto3.client('s3')
    Bucket="rawdataspotifyetl"
    Key="Before_Processed/"
    
    playlist=[]
    playlist_key=[]
    for items in s3.list_objects(Bucket=Bucket,Prefix=Key)['Contents']:
        file_name=items['Key']
        if file_name.split('.')[-1]=="json":
            response = s3.get_object(Bucket=Bucket,Key=file_name)
            content= response['Body']
            JsonObject= json.loads(content.read())
            playlist.append(JsonObject)
            playlist_key.append(file_name)
    
    for data in playlist:
        albums_list=albums(data)
        artists_list=artists(data)
        songs_list=songs(data)
        
        Album_DF=pd.DataFrame(albums_list)
        Album_DF=Album_DF.drop_duplicates(subset=['Album_ID'])
        Album_DF['Album_Release_Date']=pd.to_datetime(Album_DF['Album_Release_Date'])
        
        Artist_DF=pd.DataFrame(artists_list)
        Artist_DF=Artist_DF.drop_duplicates(subset=['artist_id'])
        
        Song_DF=pd.DataFrame(songs_list)
        Song_DF['song_added']=pd.to_datetime(Song_DF['song_added'])
        
        bucketname="transformeddataspotifyetl"
        song_key_filename ="Songs_Data/song_transformed_" +str(datetime.now()) + ".csv"
        song_buffer=StringIO()
        Song_DF.to_csv(song_buffer,index=False)
        song_content=song_buffer.getvalue()
        s3.put_object(Bucket=bucketname,Key=song_key_filename,Body=song_content)
 
        album_key_filename ="Albums_Data/album_transformed_" +str(datetime.now()) + ".csv"
        album_buffer=StringIO()
        Album_DF.to_csv(album_buffer,index=False)
        album_content=album_buffer.getvalue()
        s3.put_object(Bucket=bucketname,Key=album_key_filename,Body=album_content)
        
        artist_key_filename ="Artists_Data/artist_transformed_" +str(datetime.now()) + ".csv"
        artist_buffer=StringIO()
        Artist_DF.to_csv(artist_buffer,index=False)
        artist_content=artist_buffer.getvalue()
        s3.put_object(Bucket=bucketname,Key=artist_key_filename,Body=artist_content)
        
    s3_resource=boto3.resource('s3')
    for key in playlist_key:
        copy_resource = {
                'Bucket' : Bucket,
                'Key' : key
        }
        s3_resource.meta.client.copy(copy_resource,Bucket,"Processed/" + key.split("/")[-1])
        s3_resource.Object(Bucket,key).delete()