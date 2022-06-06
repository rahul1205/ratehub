import ijson
import uuid
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, MetaData, Table, ForeignKey, Text
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()

config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'root',
    'database': 'ratehub'
}

db_user = config.get('user')
db_pwd = config.get('password')
db_host = config.get('host')
db_port = config.get('port')
db_name = config.get('database')# specify connection string
connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'# connect to database
engine = create_engine(connection_str)
conn = engine.connect()

Session = sessionmaker(bind = engine)
session = Session()


class Amenities(Base):
    __tablename__ = 'amenities'
    id = Column(String(500) )
    name = Column(String(500), primary_key=True)
    listing = relationship("Listing", secondary='link', back_populates="amenities")

class Listing(Base):
    __tablename__ = 'listing'

    id = Column(Integer, primary_key=True)
    listing_url = Column(String(500))
    listing_name = Column(String(500))
    summary = Column(Text)
    description = Column(Text)
    property_type = Column(String(500))
    room_type = Column(String(500))
    address_street = Column(String(500))
    address_suburb = Column(String(500))
    address_government_area = Column(String(500))
    address_market = Column(String(500))
    address_country = Column(String(500))
    address_country_code = Column(String(500))
    amenities = relationship("Amenities", secondary='link', back_populates="listing")



class Link(Base):
    __tablename__ = 'link'

    listing_id = Column(Integer, ForeignKey('listing.id'), primary_key=True)
    amenities_name = Column(String(500), ForeignKey('amenities.name'), primary_key=True)


Base.metadata.create_all(engine)
# a1 = Amenities(id="1", name="bbq")
# session.add(a1)
# session.commit()



with open("listingsAndReviews.json", "rb") as f:
    for record in ijson.items(f, "item"):
        print(record['_id'])
        am_objs = []
        for am in set(record['amenities']):
            obj = session.query(Amenities).filter(Amenities.name==am).first()
            if not obj:
                obj = Amenities(id=str(uuid.uuid4()).replace("-", ""), name=am)
                session.add(obj)
                session.commit()
                am_objs.append(obj)
            else:
                am_objs.append(obj)

        obj = session.query(Listing).filter(Listing.id == record['_id']).first()

        if not obj:
            lst = Listing(id=record['_id'], listing_url=record['listing_url'], listing_name=record['name'],
                          summary=record['summary'], description=record['description'],
                          property_type=record['property_type'],
                          room_type=record['room_type'], address_street=record['address']['street'],
                          address_suburb=record['address']['suburb'],
                          address_government_area=record['address'].get('government_area', ''),
                          address_market=record['address']['market'], address_country=record['address']['country'],
                          address_country_code=record['address']['country_code']
                          )

            lst.amenities = am_objs
            session.add(lst)
            session.commit()


