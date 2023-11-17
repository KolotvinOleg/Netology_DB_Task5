import sqlalchemy as sq
import json
from sqlalchemy.orm import sessionmaker
from models import create_tables, Publisher, Book, Shop, Stock, Sale

login = 'postgres'
password = '*********'
db_name = 'netology_db'
DSN = f'postgresql://{login}:{password}@localhost:5432/{db_name}'
engine = sq.create_engine(DSN)

if __name__ == '__main__':
    create_tables(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    with open('tests_data.json') as file:
        info = json.load(file)

    for record in info:
        model = {
            'publisher' : Publisher,
            'book': Book,
            'shop': Shop,
            'stock': Stock,
            'sale': Sale
        }
        my_model = model[record['model']]
        session.add(my_model(id=record.get('pk'), **record.get('fields')))
        session.commit()

    name = input('Введите имя автора: ')
    for c in session.query(
        Book.title,
        Shop.name,
        Sale.price*Sale.count,
        Sale.date_sale)\
        .join(Publisher, Publisher.id == Book.id_publisher).join(Stock, Book.id == Stock.id_book)\
        .join(Shop, Stock.id_shop == Shop.id).join(Sale, Stock.id == Sale.id_stock)\
        .filter(Publisher.name == name).all():
        print(f'{c[0]:<40} | {c[1]:<8} | {c[2]:<6} | {c[3].strftime("%d-%m-%Y")}')

    session.close()