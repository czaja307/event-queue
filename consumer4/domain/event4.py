import json


class Type4Event:
    def __init__(self, name, data):
        self.name = name
        self.data = data

    def __str__(self):
        return f"Event({self.name}, {self.data})"

    def to_json(self):
        return json.dumps({"name": self.name, "data": self.data})

    @staticmethod
    def from_json(body):
        decoded = json.loads(body)
        return Type4Event(decoded["name"], decoded["data"])
