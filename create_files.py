import csv
import json
import os
import random
import string
from os.path import join


class DataFiles:

    def __init__(self, no_of_csvfiles, no_of_jsonfiles, no_of_txtfiles):
        self.no_of_csvfiles = no_of_csvfiles
        self.no_of_jsonfiles = no_of_jsonfiles
        self.no_of_txtfiles = no_of_txtfiles
        self.headers = ["Name", "Age", "Country"]

    def create_csv_files(self):
        dir = "csvdata/"
        os.makedirs(dir, exist_ok=True)

        for i in range(self.no_of_csvfiles):
            filename = "data" + str(i) + ".csv"
            data = self.generate_random_data(no_of_records=10)

            with open(join(dir, filename), "w", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=self.headers)
                writer.writeheader()
                writer.writerows(data)
            file.close()

    def create_json_files(self):

        dir = "jsondata/"
        os.makedirs(dir, exist_ok=True)

        for i in range(self.no_of_jsonfiles):
            filename = "data" + str(i) + ".json"
            data = self.generate_random_data(no_of_records=10)

            with open(join(dir, filename), "w") as file:
                json.dump({"headers": self.headers, "data": data}, file, indent=4)
            file.close()

    def create_txt_files(self):

        dir = "txtdata/"
        os.makedirs(dir, exist_ok=True)

        for i in range(self.no_of_txtfiles):
            filename = "data" + str(i) + ".txt"
            data = self.generate_random_data(no_of_records=10)

            with open(join(dir, filename), "w") as file:
                file.write(", ".join(self.headers) + "\n")
                for row in data:
                    file.write(f"{row['Name']}, {row['Age']}, {row['Country']}\n")
            file.close()

    def generate_random_string(self, length):
        return "".join(random.choice(string.ascii_letters) for _ in range(length))

    def generate_random_data(self, no_of_records):
        data = []
        for i in range(no_of_records):
            entry = {
                "Name": self.generate_random_string(random.randint(5, 10)),
                "Age": random.randint(20, 60),
                "Country": random.choice(COUNTRIES),
            }
            data.append(entry)
        return data

    def upload_to_s3bucket(self):
        pass


COUNTRIES = ["Abkhazia", "Afghanistan", "Albania", "Algeria", "American Samoa", "Andorra", 
             "Angola", "Anguilla", "Antigua and Barbuda", "Argentina", "Armenia", "Aruba", 
             "Australia", "Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", 
             "Barbados", "Belarus", "Belgium", "Belize", "Benin", "Bermuda", "Bhutan", 
             "Bolivia", "Bonaire", "Bosnia and Herzegovina", "Botswana", "Bouvet Island", 
             "Brazil", "British Indian Ocean Territory", "British Virgin Islands", 
             "Virgin Islands, British", "Brunei", "Brunei Darussalam", "Bulgaria", 
             "Burkina Faso", "Burundi", "Cambodia", "Cameroon", "Canada"]

obj = DataFiles(5, 5, 5)

obj.create_txt_files()
obj.create_json_files()
obj.create_csv_files()
