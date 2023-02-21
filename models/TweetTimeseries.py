from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float

Base = declarative_base()

class TweetTimeseries(Base):
    __tablename__ = "tweet_timeseries"
    id = Column(Integer, primary_key=True, not_null=True)
    tweet_id = Column(String, not_null=True)
    retweet_count = Column(Integer, not_null=True)
    favorite_count = Column(Integer, not_null=True)
    date = Column(Date, not_null=True)
    
    def __repr__(self) -> str:
        return f"UserTimeseries(id={self.id}, user_id={self.user_id}, retweet_count={self.retweet_count}, favorite_count={self.favorite_count}, date={self.date})" 