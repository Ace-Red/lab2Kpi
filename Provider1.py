from flask import Flask
from flask_restful import Resource, Api, reqparse
import psycopg2
import copy
from Filter import TicketsType, OriginFlight, DestinationFlight, Data, MaximalPrice, MinimalPrice


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class SingletonDB(metaclass=SingletonMeta):
    def __init__(self):
        self.conn = psycopg2.connect(dbname='model1', user='postgres', password='0672089596', host='localhost')

    def select_filtered_values(self):
        rows = []
        with self.conn.cursor() as cursor:
            cursor.execute('SELECT model1.public."public.Ticket".ticket_id, model1.public."public.Ticket"."seat_number", model1.public."public.Ticket"."purchase_date", model1.public."public.Ticket"."ticket_type", model1.public."public.Ticket"."price", model1.public."public.Ticket"."is_active", model1.public."public.Ticket"."user_id", model1.public."public.Ticket"."flight_id",model1.public."public.Flight"."departure_time",model1.public."public.Flight"."origin",model1.public."public.Flight"."destination" FROM model1.public."public.Ticket" INNER JOIN model1.public."public.Flight" ON model1.public."public.Flight"."flight_id" = model1.public."public.Ticket"."flight_id"')
            rows = cursor.fetchall()
        return rows


class Models(Resource):
    def get(self):
        db = SingletonDB()
        all_products = db.select_filtered_values()
        my_list = []
        for row in all_products:
            a = {"ticket_id": row[0], "seat_number": row[1], "purchase_date": str(row[2]), "ticket_type": str(row[3]),
                 "price": row[4],
                 "is_active": str(row[5]), "user_id": row[6], "flight_id": str(row[7]), "departure_time": str(row[8]),
                 "origin": str(row[9]), "destination": str(row[10])}
            my_list.append(a)
        all_products.clear()

        product_filter = TicketsType() & OriginFlight() & MaximalPrice() & MinimalPrice() & Data() & DestinationFlight()
        products = []
        for i in my_list:
            print(i)
            if product_filter.filtering_value_is_satisfied(i):
                products.append(i)
        return products


if __name__ == "__main__":
    app = Flask(__name__)
    api = Api(app)
    api.add_resource(Models, '/search/')
    app.run(port=5001, debug=True)
