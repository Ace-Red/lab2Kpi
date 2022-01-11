from flask import Flask
from flask_restful import Resource, Api, reqparse
import psycopg2
import copy


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class SingletonDB(metaclass=SingletonMeta):
    def __init__(self):
        self.conn = psycopg2.connect(dbname='model2', user='postgres', password='0672089596', host='localhost')

    def select_all_price(self):
        rows = []
        with self.conn.cursor() as cursor:
            cursor.execute(
                'SELECT model2.public."public.TicketMainInfo"."ticket_id", model2.public."public.TicketMainInfo"."ticket_type", model2.public."public.TicketMainInfo"."price" , model2.public."public.TicketMainInfo"."is_active" FROM model2.public."public.TicketMainInfo" INNER JOIN model2.public."public.TicketDetails" ON model2.public."public.TicketMainInfo"."ticket_id" = model2.public."public.TicketDetails"."ticket_details_id"')
            rows = cursor.fetchall()
        return rows

    def select_all_desc(self, i):
        rows = []
        with self.conn.cursor() as cursor:
            cursor.execute(
                'SELECT model2.public."public.TicketDetails"."ticket_details_id",  model2.public."public.TicketDetails"."seat_number", model2.public."public.TicketDetails"."purchase_date", model2.public."public.TicketDetails"."user_id", model2.public."public.TicketDetails"."flight_id", model2.public."public.TicketMainInfo"."ticket_type", model2.public."public.TicketMainInfo"."price", model2.public."public.TicketMainInfo"."is_active",model2.public."public.Flight"."origin",model2.public."public.Flight"."destination" FROM model2.public."public.TicketDetails" INNER JOIN model2.public."public.TicketMainInfo" on model2.public."public.TicketDetails"."ticket_details_id" = model2.public."public.TicketMainInfo"."ticket_details_id" INNER JOIN model2.public."public.Flight" on model2.public."public.TicketDetails"."flight_id" = model2.public."public.Flight"."flight_id"')
            rows = cursor.fetchall()
        return rows


class GetPrices(Resource):
    def get(self):
        db = SingletonDB()
        all_models = db.select_all_price()
        my_list = []
        for row in all_models:
            a = {"ticket_id": str(row[0]), "ticket_type": str(row[1]), "price": str(row[2]), "is_active": str(row[3])}
            my_list.append(a)
        return my_list


class GetDescription(Resource):
    def get(self, id):
        db = SingletonDB()
        all_models = db.select_all_desc(id)
        my_list = []
        for row in all_models:
            a = {"ticket_details_id": row[0], "seat_number": str(row[1]), "purchase_date": str(row[2]),
                 "user_id": str(row[3]),
                 "flight_id": str(row[4]), "ticket_type": str(row[5]), "price": str(row[6]),
                 "is_active": str(row[7]), "destination": str(row[8]), "origin": str(row[9])
                 }
            my_list.append(a)

        return my_list[id - 1]


if __name__ == "__main__":
    app = Flask(__name__)
    api = Api(app)
    api.add_resource(GetPrices, '/price-list/')
    api.add_resource(GetDescription, '/details/<int:id>')
    app.run(port=5002, debug=True)
