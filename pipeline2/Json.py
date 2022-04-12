import json


class Json:

    def __init__(self, user_name=None, user_id=None, location=None, employment_type=None, description=None):
        self.user_name = user_name
        self.user_id = user_id
        self.location = location
        self.employment_type = employment_type
        self.description = description
        print('init')

    def create_load_json(self):
        indent = 5
        dictionary = {
            "name": self.user_name,
            "user_id": self.user_id,
            "location": self.location,
            "employment_type": self.employment_type,
            "description": self.description
        }

        json_object = json.dumps(dictionary, indent=indent)
        new_json = json.loads(json_object)
        return new_json