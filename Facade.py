from flask_restful import Resource, Api, reqparse
from ObjectBuilder import Director, OwnObjectBuilder, Provider1ObjectBuilder, Provider2ObjectBuilder, OwnModel


class Facade:
    def __init__(self):
        self.director = Director()
        self.parser = reqparse.RequestParser()
        self.empty_model = OwnModel()

    def get_prod(self):
        director = Director()
        builder = OwnObjectBuilder()
        self.director.builder = builder
        self.director.build_filtered_model()
        own = builder.model

        builder = Provider1ObjectBuilder()
        self.director.builder = builder
        self.director.build_filtered_model()
        service1 = builder.model

        builder = Provider2ObjectBuilder()
        self.director.builder = builder
        self.director.build_filtered_model()
        service2 = builder.model
        own.join(service1)
        own.join(service2)
        return own.models

    def insert(self):
        self.parser.add_argument("seat_number")
        self.parser.add_argument("purchase_date")
        self.parser.add_argument("ticket_type")
        self.parser.add_argument("price")
        self.parser.add_argument("is_active")
        self.parser.add_argument("user_id")
        self.parser.add_argument("flight_id")
        self.parser.add_argument("departure_time")
        self.parser.add_argument("origin")
        self.parser.add_argument("destination")

        args = self.parser.parse_args()
        self.empty_model.insert(args)

    def delete(self):
        self.parser.add_argument("ticket_id")
        args = self.parser.parse_args()
        self.empty_model.delete(args["ticket_id"])

    def update(self):
        self.parser.add_argument("ticket_id")
        self.parser.add_argument("seat_number")
        self.parser.add_argument("purchase_date")
        self.parser.add_argument("ticket_type")
        self.parser.add_argument("price")
        self.parser.add_argument("is_active")
        self.parser.add_argument("user_id")
        self.parser.add_argument("flight_id")
        self.parser.add_argument("departure_time")
        self.parser.add_argument("origin")
        self.parser.add_argument("destination")

        args = self.parser.parse_args()
        self.empty_model.update(args)
