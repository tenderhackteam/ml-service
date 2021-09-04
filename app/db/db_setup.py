from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    ARRAY,
    JSON,
    Float,
    ForeignKey
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import os, asyncio

Base = declarative_base()

class Category(Base):
    __tablename__ = 'category'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    items = relationship("Item", backref="category")

class Item(Base):
    __tablename__ = 'item'
    id = Column(Integer, primary_key=True)
    cte_id = Column(Integer)
    cte_name = Column(String)
    category_id = Column(Integer, ForeignKey("category.id"))
    description = Column(String)
    cte_props = Column(JSON) 
    regions = Column(ARRAY(String))
    made_contracts = Column(Integer)
    suppliers = Column(JSON) 
    country = Column(String)
    other_items_in_contracts = Column(String)
    cpgz_id = Column(Float)
    cpgz_code  = Column(String)
    model = Column(String)
    price = Column(JSON)


# async_engine = create_async_engine(
#     os.environ.get('PSQL_DB')
# )
engine = create_engine(
    os.environ.get('PSQL_DB')
)
Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.bind =engine
Base.metadata.create_all()
# async_session = sessionmaker(
#     async_engine, expire_on_commit=False, class_=AsyncSession
# )

# async def create_all(engine, meta):
#     async with engine.begin() as conn:
#         await conn.run_sync(meta.create_all)

# asyncio.run(create_all(async_engine, Base.metadata))

#Base.metadata.bind = engine
#Base.metadata.create_all(engine)

#DBSession = sessionmaker(bind=engine)
#session = DBSession()
