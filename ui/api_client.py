import requests

class APIClient:
    def __init__(self):
        self.token = None

    def set_token(self, token):
        self.token = token

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}"} if self.token else {}

    def login(self, username, password):
        r = requests.post(
            "http://localhost:7000/login",
            json={"username": username, "password": password}
        )
        return r

    def get_patients(self, hospital_url):
        return requests.get(
            f"{hospital_url}/patients",
            headers=self._headers()
        )

    def create_patient(self, hospital_url, data):
        return requests.post(
            f"{hospital_url}/patient",
            json=data,
            headers=self._headers()
        )

    def get_patient(self, hospital_url, pid):
        return requests.get(
            f"{hospital_url}/patient/{pid}",
            headers=self._headers()
        )

    def add_visit(self, hospital_url, pid, data):
        return requests.post(
            f"{hospital_url}/patient/{pid}/visit",
            json=data,
            headers=self._headers()
        )

    def delete_patient(self, hospital_url, pid):
        return requests.delete(
            f"{hospital_url}/patient/{pid}",
            headers=self._headers()
        )
    
    def status(self, hospital_url):
        return requests.get(
            f"{hospital_url}/status"
        )