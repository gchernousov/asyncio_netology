import asyncio
import os
from aiohttp import ClientSession
from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from more_itertools import chunked
from dotenv import load_dotenv
from datetime import datetime


# connect
load_dotenv()
db_name = os.getenv('POSTGRES_DB')
db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')
db_host = os.getenv('HOST')
db_port = os.getenv('PORT')

DSN = f'postgresql+asyncpg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_async_engine(DSN)
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

ROOT_URL = 'https://swapi.dev/api/people/'
CHUNK_SIZE = 10


# models
Base = declarative_base()


class PersonModel(Base):
    __tablename__ = "person"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    eye_color = Column(String)
    skin_color = Column(String)
    birth_year = Column(String)
    homeworld = Column(String)
    height = Column(String)
    mass = Column(String)
    films = Column(Text)
    species = Column(Text)
    starships = Column(Text)
    vehicles = Column(Text)


# functions
async def chunked_async(async_iter, size):
    results = []
    count = 0
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            break
        if item.get('status'):
            count += 1
        results.append(item)
        if count == size:
            break
        if len(results) == size:
            yield results
            results = []


async def get_person(person_id: int, session: ClientSession):
    async with session.get(f'{ROOT_URL}{person_id}') as response:
        if response.status == 404:
            return {'status': 404}
        json_data = await response.json()
        return json_data


async def insert_person(person_chunk):
    async with Session() as session:
        for person in person_chunk:
            if person.get('status') == 404:
                break
            new_person = PersonModel(
                name=person['name'],
                gender=person['gender'],
                hair_color=person['hair_color'],
                eye_color=person['eye_color'],
                skin_color=person['skin_color'],
                birth_year=person['birth_year'],
                homeworld=person['homeworld'],
                height=person['height'],
                mass=person['mass'],
                films=','.join(person['films']),
                species=','.join(person['species']),
                starships=','.join(person['starships']),
                vehicles=','.join(person['vehicles'])
            )
            session.add(new_person)
            await session.commit()


async def get_people(number: int):
    async with ClientSession() as session:
        for chunk in chunked(range(1, number+1), CHUNK_SIZE):
            coroutines = [get_person(person_id=i, session=session) for i in chunk]
            results = await asyncio.gather(*coroutines)
            for item in results:
                yield item


async def main(number):
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
        await connection.commit()
    async for chunk in chunked_async(get_people(number), CHUNK_SIZE):
        asyncio.create_task(insert_person(chunk))
    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task


if __name__ == "__main__":

    start = datetime.now()
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main(120))
    print(datetime.now() - start)
    print('done!')
