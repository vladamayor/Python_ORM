import sqlalchemy
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
from sqlalchemy.orm import sessionmaker
import json
from pprint import pprint
from models import create_tables, Publisher, Book, Stock, Sale, Shop


login = os.getenv('LOGIN')
passw = os.getenv('PASS')
DSN = 'postgresql://%(login)s:%(passw)s@localhost:5432/publisher'%{'login': login,'passw': passw}
engine = sqlalchemy.create_engine(DSN)

create_tables(engine)

Session = sessionmaker(bind=engine)
session =Session()


with open ('fixtures/test_data.json', 'r') as file:
    data = json.load(file)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()

def show_info(publ):

    if type(publ) == str:
        publ = session.query(Publisher.id).filter(Publisher.name.ilike(publ)).all()[0][0]

    subq = session.query(Book.id, Book.title).join(Publisher.books).filter(Publisher.id == publ).subquery()

    for c in session.query(subq.c.title, Shop.name, Sale.price, Sale.date_sale
                       ).join(Stock.shops_2).join(Stock.sales).join(subq, Stock.id_book == subq.c.id).all():
        print(f'{c[0]} | {c[1]} | {c[2]} | {c[3]}')


show_info()


session.close()