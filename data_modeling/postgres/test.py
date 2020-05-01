import pandas as pd 

df = pd.read_json('/home/ana/projects/mine/data-eng/data_modeling/postgres/data/song_data/A/A/A/TRAAAVO128F93133D4.json', lines=True)
artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
print(artist_data)