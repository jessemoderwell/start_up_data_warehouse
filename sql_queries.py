import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop  table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

songplay_table_create = ("""create table if not exists songplays (songplay_id int IDENTITY(0,1), start_time bigint not null, user_id int not null, level varchar, song_id varchar, artist_id varchar, session_id varchar, location varchar, user_agent varchar, primary key(songplay_id))
""")

user_table_create = ("""create table if not exists users (user_id int not null, first_name varchar, last_name varchar, gender varchar, level varchar, primary key (user_id))
""")

song_table_create = ("""create table if not exists songs (song_id varchar not null, title varchar, artist_id varchar, year int, duration numeric, primary key (song_id)) 
""")

artist_table_create = ("""create table if not exists artists (artist_id varchar not null, name varchar, location varchar, latitude numeric, longitude numeric, primary key (artist_id))
""")

time_table_create = ("""create table if not exists time (start_time bigint not null, hour int, day int, week int, month int, year int, weekday int, primary key (start_time))
""")
# STAGING TABLES

staging_events_copy = ("""copy staging_events
from 's3://udacity-dend/log_data'
iam_role '{}'
json 's3://udacity-dend/log_json_path.json' 
compupdate off region 'us-west-2'
""").format(config['IAM_ROLE']['ARN'])

staging_events_table_create = ("""create table if not exists staging_events (artist varchar, auth varchar, firstname varchar, gender varchar(1), iteminsession int, lastname varchar, length numeric, level varchar, location varchar, method varchar, page varchar, registration bigint, sessionid int, song varchar, status int, ts bigint, useragent varchar, userid int)""")

staging_songs_copy = ("""copy staging_songs
from 's3://udacity-dend/song_data'
iam_role '{}'
json 'auto' compupdate off region 'us-west-2'
""").format(config['IAM_ROLE']['ARN'])

staging_songs_table_create = ("""create table if not exists staging_songs (num_songs int, artist_id varchar, artist_latitude numeric, artist_longitude numeric, artist_location varchar, artist_name varchar, song_id varchar, title varchar, duration numeric, year int)""")

# FINAL TABLES

songplay_table_insert = ("""insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
select distinct e.ts, e.userId, e.level, s.song_id, s.artist_id, sessionid, 
s.artist_location, e.userAgent from staging_events e
inner join staging_songs s on concat(e.song, e.artist) = concat(s.title, s.artist_name)
where e.page = 'NextSong'
and e.userId is not null
""")

user_table_insert = ("""insert into users (user_id, first_name, last_name, gender, level) 
select distinct userid, firstname, lastname, gender, level
from staging_events
where userid is not null
""")

song_table_insert = ("""insert into songs (song_id, artist_id, year, duration, title) 
select distinct song_id, artist_id, 
case when year = 0 then null else year end, 
duration, title
from staging_songs
where song_id is not null
""")

artist_table_insert = ("""insert into artists (artist_id, name, location, latitude, longitude) 
select distinct artist_id, artist_name, artist_location, 
artist_latitude, artist_longitude
from staging_songs
where artist_id is not null
""")

time_table_insert = ("""insert into time (start_time, hour, day, week, month, year, weekday) 
(select distinct ts, 
extract(hour from date_add('ms',ts,'1970-01-01')), 
extract(day from date_add('ms',ts,'1970-01-01')), 
extract(week from date_add('ms',ts,'1970-01-01')), 
extract(month from date_add('ms',ts,'1970-01-01')), 
extract(year from date_add('ms',ts,'1970-01-01')), 
extract(dow from date_add('ms',ts,'1970-01-01'))
from staging_events
where ts is not null)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
