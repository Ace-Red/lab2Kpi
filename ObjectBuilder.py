from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict
from Database import SingletonDB
import requests
from Filter import MaximalPrice, MinimalPrice, Data, TicketsType, OriginFlight, DestinationFlight


class ObjectBuilder(ABC):
    @property
    @abstractmethod
    def model(self) -> None:
        pass

    @abstractmethod
    def extract_from_source(self) -> None:
        pass

    @abstractmethod
    def reformat(self) -> None:
        pass

    @abstractmethod
    def filter(self) -> None:
        pass


class Provider1ObjectBuilder(ObjectBuilder):
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self._model = OwnModel()

    @property
    def model(self) -> OwnModel:
        model = self._model
        self.reset()
        return model

    def extract_from_source(self) -> None:
        self._model.set(requests.get('http://127.0.0.1:5001/search/').json())

    def reformat(self) -> None:
        pass

    def filter(self) -> None:
        self._model.filter()


class Provider2ObjectBuilder(ObjectBuilder):
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self._model = OwnModel()

    @property
    def model(self) -> OwnModel:
        model = self._model
        self.reset()
        return model

    def extract_from_source(self) -> None:
        self._model.set(requests.get('http://127.0.0.1:5002/price-list/').json())

    def reformat(self) -> None:
        full_models = []
        for row in self._model.models:
            full_models.append(requests.get('http://127.0.0.1:5002/details/' + str(row["ticket_id"])).json())
        self._model.set(full_models)

    def filter(self) -> None:
        self._model.filter()


class OwnObjectBuilder(ObjectBuilder):
    def __init__(self) -> None:
        self.reset()
        self.db = SingletonDB()

    def reset(self) -> None:
        self._model = OwnModel()

    @property
    def model(self) -> OwnModel:
        model = self._model
        self.reset()
        return model

    def extract_from_source(self) -> None:
        self._model.set(self._model.select_all_db_data())

    def reformat(self) -> None:
        my_list = []
        for row in self.model.models:
            a = {"ticket_id": row[0], "seat_number": row[1], "purchase_date": str(row[2]), "user_id": row[3],
                 "flight_id": row[4], "departure_time": str(row[5]), "origin": str(row[6]),
                 "destination": str(row[7]), "ticket_type": str(row[8]), "price": row[9],
                 "is_active": str(row[10])
                 }
            my_list.append(a)
        self._model.set(my_list)

    def filter(self) -> None:
        self._model.filter()


class Director:
    def __init__(self) -> None:
        self._builder = None

    @property
    def builder(self) -> ObjectBuilder:
        return self._builder

    @builder.setter
    def builder(self, builder: ObjectBuilder) -> None:
        self._builder = builder

    def build_all_models(self) -> None:
        self.builder.extract_from_source()
        self.builder.reformat()

    def build_filtered_model(self) -> None:
        self.builder.extract_from_source()
        self.builder.reformat()
        self.builder.filter()


class OwnModel:
    def __init__(self):
        self.models = []
        self.conn = SingletonDB().conn

    def add(self, model: Dict[str, Any]):
        self.models.append(model)

    def join(self, another_model):
        self.models += another_model.models

    def drop(self, id):
        del self.models[id]

    def set(self, models):
        self.models = models

    def select_all_db_data(self):
        rows = []
        with self.conn.cursor() as cursor:
            cursor.execute(
                'SELECT "FlightLabKpi".public."public.Ticket".ticket_id, "FlightLabKpi".public."public.Ticket"."seat_number", "FlightLabKpi".public."public.Ticket"."purchase_date", "FlightLabKpi".public."public.Ticket"."user_id", "FlightLabKpi".public."public.Ticket"."flight_id", "FlightLabKpi".public."public.Flight"."departure_time", "FlightLabKpi".public."public.Flight"."origin", "FlightLabKpi".public."public.Flight"."destination","FlightLabKpi".public."public.Ticket"."ticket_type","FlightLabKpi".public."public.Ticket"."price","FlightLabKpi".public."public.Ticket"."is_active" FROM "FlightLabKpi".public."public.Ticket" INNER JOIN "FlightLabKpi".public."public.Flight" ON "FlightLabKpi".public."public.Flight"."flight_id" = "FlightLabKpi".public."public.Ticket"."flight_id"')
            rows = cursor.fetchall()
        return rows

    def insert(self, args):
        with self.conn.cursor() as cursor:
            cursor.execute(
                '''INSERT INTO "public.Ticket"("seat_number", "purchase_date", "ticket_type", "price", "is_active", "user_id", "flight_id") VALUES(%s,'%s','%s',%s,%s,%s,%s)''' % (
                    str(args["seat_number"]), str(args["purchase_date"]), str(args["ticket_type"]),
                    str(args["price"]),
                    str(args["is_active"]),
                    str(args["user_id"]), str(args["flight_id"])))
        self.conn.commit()

    def delete(self, id):
        with self.conn.cursor() as cursor:
            cursor.execute('DELETE FROM "public.Ticket" WHERE "ticket_id"=' + str(id))
        self.conn.commit()

    def update(self, args):
        query_str = 'UPDATE "public.Ticket" SET '
        for key, value in args.items():
            if key != 'ticket_id' and value is not None:
                query_str += '"' + key + '"=' + "'" + str(value) + "',"
        query_str = query_str[0:-1]
        query_str += ' WHERE "ticket_id"=' + str(args["ticket_id"])
        with self.conn.cursor() as cursor:
            cursor.execute(query_str)
        self.conn.commit()

    def filter(self):
        model_filter = MaximalPrice() & MinimalPrice() & Data() & TicketsType() & OriginFlight() & DestinationFlight()
        models = []
        for i in self.models:
            if model_filter.filtering_value_is_satisfied(i):
                models.append(i)
        self.models = models
