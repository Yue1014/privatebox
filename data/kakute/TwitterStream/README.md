# Crawling dataset from twitter

1. install tweepy
```
pip install tweepy
```

2. modify config.py with your own configuration

3. run the script, the data is put in data/stream_"".json. For example, if you want to produce the list of tweets for the query "apple", run
```
python twitter_download.py -q apple -d data
```
