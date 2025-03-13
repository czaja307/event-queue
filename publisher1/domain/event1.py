class Type1Event:
    def __init__(self, name, data):
        self.name = name
        self.data = data

    def __str__(self):
        return f"Event({self.name}, {self.data})"

    def to_json(self):
        return f'{{"name": "{self.name}", "data": {self.data}}}'
