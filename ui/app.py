import time

import streamlit as st
import docker
import os
from api_client import APIClient
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if "client" not in st.session_state:
    st.session_state.client = APIClient()

client = st.session_state.client
docker_client = docker.from_env()

HOSPITALS = {
    "Hospital 1": "http://localhost:5001",
    "Hospital 2": "http://localhost:5002",
    "Hospital 3": "http://localhost:5003"
}

services = [
    ("hospital1", "patient_service.py", 5001),
    ("hospital2", "patient_service.py", 5002),
    ("hospital3", "patient_service.py", 5003),

    ("hospital1", "audit_service.py", 6001),
    ("hospital2", "audit_service.py", 6002),
    ("hospital3", "audit_service.py", 6003),

    ("hospital1", "ml_worker.py", 7001),
    ("hospital2", "ml_worker.py", 7002),
    ("hospital3", "ml_worker.py", 7003),

    ("iam", "iam_service.py", 7000)
]


def start_services():
    try:
        docker_client.networks.get("hospital_net")
    except:
        docker_client.networks.create("hospital_net", driver="bridge")


    for folder, script, port in services:

        container_name = f"{folder}_{script.replace('.py','')}"

        try:
            docker_client.containers.get(container_name)
            st.warning(f"{container_name} already running")
            continue
        except Exception:
            pass

        path = os.path.join(PROJECT_ROOT, folder)

        st.write(f"Starting {container_name} on port {port}")

        docker_client.containers.run(
            image="service_build",
            command=f"python {script}",
            name=container_name,
            hostname=folder,
            network="hospital_net",
            volumes={
                path: {
                    "bind": "/app",
                    "mode": "rw"
                }
            },
            working_dir="/app",
            ports={
                f"{port}/tcp": port,
                f"{port+10}/tcp": port+10
            },
            detach=True
        )
    with st.spinner("Waiting for Docker to wake up and Raft to elect a leader..."):
        time.sleep(10)

st.title("Distributed EHRS UI")

if "page" not in st.session_state:
    st.session_state.page = "view_patients"

if "token" not in st.session_state:
    st.session_state.token = None

if not st.session_state.token:
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Start Services"):
        start_services()
        st.success("All services up and running")

    if st.button("Login"):
        try:
            response = client.login(username, password)
            token = response.json().get("token")
            if token:
                st.session_state.token = token
                client.set_token(token)
                st.success("Logged in successfully")
            else:
                st.error("Login failed: no token received")
        except Exception as e:
            st.error(f"Login failed: {e}")

else:
    st.header("Hospital Databases")

    st.sidebar.title("Navigation")

    if st.sidebar.button("View Patients"):
        st.session_state.page = "view_patients"

    if st.sidebar.button("Add Patient"):
        st.session_state.page = "add_patient"

    if st.sidebar.button("Add Visit"):
        st.session_state.page = "add_visit"
    
    if st.sidebar.button("Delete Patient"):
        st.session_state.page = "delete_patient"

    if st.session_state.page == "view_patients":
        cols = st.columns(3)
        for i, (name, url) in enumerate(HOSPITALS.items()):

            with cols[i]:

                st.subheader(name)

                if st.button("Status", key=f"status_{name}"):
                    r = client.status(url)
                    if r.status_code == 200:
                        data = r.json()
                        st.json(data)
                    else: 
                        st.error("Failed to get status")

                if st.button(f"Load {name}", key=name):

                    r = client.get_patients(url)

                    if r.status_code == 200:
                        data = r.json()
                        st.write(f"Patients: {data['total']}")
                        st.json(data["patients"])
                    else:
                        st.error("Request failed")
    
    if st.session_state.page == "add_patient":
        st.header("Add new patient")

        hospital_name = st.selectbox("Hospital", list(HOSPITALS.keys()))
        name = st.text_input("Name")
        dob = st.text_input("Date of Birth")
        gender = st.selectbox("Gender", ["M", "F", "Other"])
        ssn = st.text_input("SSN")

        if st.button("Create Patient"):
            payload = {"name": name, "dob": dob, "gender": gender, "ssn": ssn}
            hospital_url = HOSPITALS[hospital_name]
            r = client.create_patient(hospital_url, payload)
            if r.status_code == 201:
                st.success(f"Patient created in {hospital_name}")
            else:
                st.error(r.text)
    
    if st.session_state.page == "delete_patient":
        st.header("Delete Patient")

        hospital_name = st.selectbox(
            "Hospital",
            list(HOSPITALS.keys()),
            key="visit_hospital_select"
        )


        hospital_url = HOSPITALS[hospital_name]
        pid = st.text_input("Patient ID")

        if st.button("Delete Patient"):

            if not hospital_url or not pid:
                st.error("Patient ID required")
            else:
                resp = client.delete_patient(hospital_url, pid)

                if resp.status_code == 200:
                    st.success("Patient deleted")
                elif resp.status_code == 404:
                    st.error("Patient not found")
                else:
                 st.error(f"Error: {resp.status_code} - {resp.text}")

    
    if st.session_state.page == "add_visit":
        st.header("Add Patient Visit")

        hospital_name = st.selectbox(
            "Hospital",
            list(HOSPITALS.keys()),
            key="visit_hospital_select"
        )


        hospital_url = HOSPITALS[hospital_name]
        pid = st.text_input("Patient ID")

        reason = st.text_input("Reason for Visit")
        doctor = st.text_input("Doctor")
        notes = st.text_area("Notes")

        if st.button("Add Visit", key="visit_btn"):

            if not hospital_url or not pid:
                st.error("Patient ID required")

            else:
                visit_data = {
                    "reason": reason,
                    "doctor": doctor,
                    "notes": notes
                }

                resp = client.add_visit(hospital_url, pid, visit_data)

                if resp.status_code == 200:
                    st.success("Visit added")
                elif resp.status_code == 404:
                    st.error("Patient not found")
                else:
                    st.error(f"Error: {resp.status_code} - {resp.text}")
