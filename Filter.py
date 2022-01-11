from flask_restful import reqparse


class Specification:

    def __and__(self, other):
        return And(self, other)

    def __or__(self, other):
        return Or(self, other)

    def filtering_value_is_satisfied(self, candidate):
        raise NotImplementedError()


class And(Specification):
    def __init__(self, *specifications):
        self.specifications = specifications

    def __and__(self, other):
        if isinstance(other, And):
            self.specifications += other.specifications
        else:
            self.specifications += (other,)
        return self

    def filtering_value_is_satisfied(self, candidate):
        satisfied = all([
            specification.filtering_value_is_satisfied(candidate)
            for specification in self.specifications
        ])
        return satisfied


class Or(Specification):
    def __init__(self, *specifications):
        self.specifications = specifications

    def __or__(self, other):
        if isinstance(other, Or):
            self.specifications += other.specifications
        else:
            self.specifications += (other,)
        return self

    def filtering_value_is_satisfied(self, candidate):
        satisfied = any([
            specification.filtering_value_is_satisfied(candidate)
            for specification in self.specifications
        ])
        return satisfied


class TicketsType(Specification):
    def filtering_value_is_satisfied(self, ticketFlight):
        parser = reqparse.RequestParser()
        parser.add_argument("ticket_type")
        args = parser.parse_args()
        if args['ticket_type']:
            return ticketFlight['ticket_type'] == args['ticket_type']
        else:
            return True


class OriginFlight(Specification):
    def filtering_value_is_satisfied(self, ticketFlight):
        parser = reqparse.RequestParser()
        parser.add_argument("origin")
        args = parser.parse_args()
        if args['origin']:
            return ticketFlight['origin'] == args['origin']
        else:
            return True


class DestinationFlight(Specification):
    def filtering_value_is_satisfied(self, ticketFlight):
        parser = reqparse.RequestParser()
        parser.add_argument("destination")
        args = parser.parse_args()
        if args['destination']:
            return ticketFlight['destination'] == args['destination']
        else:
            return True


class Data(Specification):
    def filtering_value_is_satisfied(self, ticketFlight):
        parser = reqparse.RequestParser()
        parser.add_argument("date")
        args = parser.parse_args()
        if args['date']:
            return args['date'] in ticketFlight['departure_time']
        else:
            return True


class MinimalPrice(Specification):
    def filtering_value_is_satisfied(self, ticketFlight):
        parser = reqparse.RequestParser()
        parser.add_argument("minPrice")
        args = parser.parse_args()
        if args['minPrice']:
            return ticketFlight['price'] >= int(args['minPrice'])
        else:
            return True


class MaximalPrice(Specification):
    def filtering_value_is_satisfied(self, ticketFlight):
        parser = reqparse.RequestParser()
        parser.add_argument("maxPrice")
        args = parser.parse_args()
        if args['maxPrice']:
            return ticketFlight['price'] <= int(args['maxPrice'])
        else:
            return True
