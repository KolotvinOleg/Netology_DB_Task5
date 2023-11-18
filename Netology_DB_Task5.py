import sqlalchemy as sq
import json
from sqlalchemy.orm import sessionmaker
from models import create_tables, Publisher, Book, Shop, Stock, Sale


def insert_data(file_name):
    with open(file_name) as file:
        info = json.load(file)

    for record in info:
        model = {
            'publisher': Publisher,
            'book': Book,
            'shop': Shop,
            'stock': Stock,
            'sale': Sale
        }
        my_model = model[record['model']]
        session.add(my_model(id=record.get('pk'), **record.get('fields')))
        session.commit()


def get_shops(author) -> None:
    my_query = session.query(
        Book.title,
        Shop.name,
        Sale.price * Sale.count,
        Sale.date_sale). \
        join(Publisher, Publisher.id == Book.id_publisher). \
        join(Stock, Book.id == Stock.id_book). \
        join(Shop, Stock.id_shop == Shop.id).join(Sale, Stock.id == Sale.id_stock)
    if author.isdigit():
        result = my_query.filter(Publisher.id == author).all()
    else:
        result = my_query.filter(Publisher.name == author).all()
    for title, name, sale, date in result:
        print(f'{title:<40} | {name:<8} | {sale:<6} | {date.strftime("%d-%m-%Y")}')


login = 'postgres'
password = '**********'
db_name = 'netology_db'
DSN = f'postgresql://{login}:{password}@localhost:5432/{db_name}'
engine = sq.create_engine(DSN)

if __name__ == '__main__':
    create_tables(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    insert_data('tests_data.json')
    author = input('Введите имя или номер автора: ')
    get_shops(author)
    session.close()
