import streamlit as st
from cryptography.fernet import Fernet
import teradatasql
import pyodbc
import requests
import os

# Persistent encryption key setup
KEY_FILE = "fernet.key"

def load_or_generate_key():
    if os.path.exists(KEY_FILE):
        with open(KEY_FILE, "rb") as f:
            return f.read()
    else:
        key = Fernet.generate_key()
        with open(KEY_FILE, "wb") as f:
            f.write(key)
        return key

if "encryption_key" not in st.session_state:
    st.session_state["encryption_key"] = load_or_generate_key()

fernet = Fernet(st.session_state["encryption_key"])

# Encryption helpers
def encrypt_credential(credential: str) -> str:
    return fernet.encrypt(credential.encode()).decode()

def decrypt_credential(encrypted_credential: str) -> str:
    try:
        return fernet.decrypt(encrypted_credential.encode()).decode()
    except Exception:
        return ""

# Store credentials and track connection names
def store_credentials(source_type: str, conn_name: str, **kwargs):
    for key, value in kwargs.items():
        st.session_state[f"{source_type}_{conn_name}_{key}"] = encrypt_credential(value)
    conn_list_key = f"{source_type}_connections"
    if conn_list_key not in st.session_state:
        st.session_state[conn_list_key] = []
    if conn_name not in st.session_state[conn_list_key]:
        st.session_state[conn_list_key].append(conn_name)

# Retrieve credentials
def get_credentials(source_type: str, conn_name: str, keys: list) -> dict:
    return {
        key: decrypt_credential(st.session_state.get(f"{source_type}_{conn_name}_{key}", ""))
        for key in keys
    }

# List saved connections
def get_saved_connections(source_type: str) -> list:
    return st.session_state.get(f"{source_type}_connections", [])

# Teradata connection
def get_teradata_connection(conn_name="default"):
    try:
        creds = get_credentials("teradata", conn_name, ["host", "user", "password"])
        return teradatasql.connect(
            host=creds["host"],
            user=creds["user"],
            password=creds["password"]
        )
    except Exception as e:
        st.error(f"Failed to connect to Teradata: {e}")
        return None

# Azure SQL DB connection
def get_azure_sql_connection(conn_name="default"):
    try:
        creds = get_credentials("azuresql", conn_name, ["server", "database", "user", "password"])
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={creds['server']};DATABASE={creds['database']};"
            f"UID={creds['user']};PWD={creds['password']}"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        st.error(f"Failed to connect to Azure SQL DB: {e}")
        return None

# Databricks catalog access
def get_databricks_catalog(conn_name="default"):
    try:
        creds = get_credentials("databricks", conn_name, ["workspace_url", "access_token"])
        headers = {"Authorization": f"Bearer {creds['access_token']}"}
        response = requests.get(f"{creds['workspace_url']}/api/2.0/unity-catalog/catalogs", headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Databricks API error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        st.error(f"Failed to access Databricks catalog: {e}")
        return None

# UI for storing credentials
def run_connector_ui():
    st.title("Setup Connector")
    source = st.selectbox("Select Connection type", ["Teradata", "Azure SQL DB", "Databricks"])
    source_key_map = {
        "Teradata": "teradata",
        "Azure SQL DB": "azuresql",
        "Databricks": "databricks"
    }
    source_key = source_key_map[source]
    saved_conns = get_saved_connections(source_key)
    mode = st.radio("Choose Mode", ["Create New Connection", "Edit Existing Connection"])

    if mode == "Edit Existing Connection" and saved_conns:
        conn_name = st.selectbox("Select Connection to Edit", saved_conns)
    else:
        conn_name = st.text_input("Enter New Connection Name", value="")

    existing_creds = {}
    if mode == "Edit Existing Connection" and conn_name in saved_conns:
        if source_key == "teradata":
            existing_creds = get_credentials(source_key, conn_name, ["host", "user", "password"])
        elif source_key == "azuresql":
            existing_creds = get_credentials(source_key, conn_name, ["server", "database", "user", "password"])
        elif source_key == "databricks":
            existing_creds = get_credentials(source_key, conn_name, ["workspace_url", "access_token"])

    if source == "Teradata":
        host = st.text_input("Teradata Host", value=existing_creds.get("host", ""))
        user = st.text_input("Teradata Username", value=existing_creds.get("user", ""))
        password = st.text_input("Teradata Password", type="password", value=existing_creds.get("password", ""))
        if st.button("Save Teradata Credentials"):
            if host and user and password and conn_name:
                store_credentials("teradata", conn_name, host=host, user=user, password=password)
                st.success(f"Teradata credentials saved/updated as '{conn_name}'.")
            else:
                st.error("Please fill in all fields.")
    elif source == "Azure SQL DB":
        server = st.text_input("Azure SQL Server", value=existing_creds.get("server", ""))
        database = st.text_input("Database Name", value=existing_creds.get("database", ""))
        user = st.text_input("Username", value=existing_creds.get("user", ""))
        password = st.text_input("Password", type="password", value=existing_creds.get("password", ""))
        if st.button("Save Azure SQL Credentials"):
            if server and database and user and password and conn_name:
                store_credentials("azuresql", conn_name, server=server, database=database, user=user, password=password)
                st.success(f"Azure SQL credentials saved/updated as '{conn_name}'.")
            else:
                st.error("Please fill in all fields.")
    elif source == "Databricks":
        workspace_url = st.text_input("Databricks Workspace URL", value=existing_creds.get("workspace_url", ""))
        access_token = st.text_input("Access Token", type="password", value=existing_creds.get("access_token", ""))
        if st.button("Save Databricks Credentials"):
            if workspace_url and access_token and conn_name:
                store_credentials("databricks", conn_name, workspace_url=workspace_url, access_token=access_token)
                st.success(f"Databricks credentials saved/updated as '{conn_name}'.")
            else:
                st.error("Please fill in all fields.")
