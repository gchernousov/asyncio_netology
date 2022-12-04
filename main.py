import asyncio
import os
from aiohttp import ClientSession
from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from more_itertools import chunked
from dotenv import load_dotenv


# connect
load_dotenv()
db_name = os.getenv('POSTGRES_DB')
db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')
db_host = os.getenv('HOST')
db_port = os.getenv('PORT')

DSN = f'postgresql+asyncpg://{db_name}:{db_password}@{db_host}:{db_port}/{db_name}'
# engine = create_async_engine(DSN)
# Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

ROOT_URL = 'https://www.swapi.tech/api/people/'
CHUNK_SIZE = 4

# models
# Base = declarative_base()


# class PersonModel(Base):
#     __tablename__ = "person"
#
#     id = Column(Integer, primary_key=True)
#     id_swapi = Column(Integer, nullable=False, unique=True)
#     name = Column(String(100))
#     gender = Column(String(12))
#     hair_color = Column(String(24))
#     eye_color = Column(String(24))
#     skin_color = Column(String(24))
#     birth_year = Column(String(12))
#     homeworld = Column(String(100))
#     height = Column(Integer)
#     mass = Column(Integer)
#     films = Column(Text)
#     species = Column(Text)
#     starships = Column(Text)
#     venicles = Column(Text)


# functions
async def chunked_async(async_iter, size):
    results = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            break
        results.append(item)
        if len(results) == size:
            yield results
            results = []


async def get_person(person_id: int, session: ClientSession):
    print(f'begin > {person_id}')
    async with session.get(f'{ROOT_URL}{person_id}') as response:
        json_data = await response.json()
    print(f'end > {person_id}')
    return json_data


async def get_people(number: int):
    async with ClientSession() as session:
        for chunk in chunked(range(1, number+1), CHUNK_SIZE):
            print(f'CHUNK = {chunk}')
            coroutines = [get_person(person_id=i, session=session) for i in chunk]
            print(f'COROUTINES = {coroutines}')
            results = await asyncio.gather(*coroutines)
            for item in results:
                yield item


async def main():

    # async with engine.begin() as connection:
    #     await connection.run_sync(Base.metadata.create_all)
    #     await connection.commit()

    async for chunk in chunked_async(get_people(10), CHUNK_SIZE):
        print(f'RESULTS = {chunk}')


if __name__ == "__main__":

    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
