import streamlit as st
from cryptography.fernet import Fernet
import importlib
import openai
import os

# Set page configuration
st.set_page_config(page_title="Unified Data Utility App", layout="wide")

# Sidebar navigation
st.sidebar.title("Navigation")
tabs = [
    "Home",
    "Data Profiling & Visualization",
    "Data Lineage",
    "Script Generator",
    "Data Quality Checks"
]
selected_tab = st.sidebar.radio("Go to", tabs)

# Chat toggle (always visible)
with st.sidebar.expander("Chat Assistant", expanded=False):
    if "show_chat" not in st.session_state:
        st.session_state.show_chat = True  # Default to enabled
    show_chat = st.checkbox("Show Chat Panel", value=st.session_state.show_chat)

# Connector toggle (always visible)
with st.sidebar.expander("Connector Panel", expanded=False):
    show_connector = st.checkbox("Show Connector Panel")

# Azure OpenAI Configuration
openai.api_type = "azure"
openai.api_base = "https://aiarhcgopenai.openai.azure.com/"
openai.api_version = "2023-03-15-preview"
openai.api_key = st.secrets["OPENAI_API_KEY"]

# Initialize session state
if "messages1" not in st.session_state:
    st.session_state.messages1 = []

if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "system", "content": "Your own assistant."}]

if "chat_input_key" not in st.session_state:
    st.session_state.chat_input_key = 0

if "option_input_key" not in st.session_state:
    st.session_state.option_input_key = 0

# Load feature modules
script_generator = importlib.import_module("script_generator")
data_lineage = importlib.import_module("data_lineage")
data_profiling = importlib.import_module("data_profiling")
data_quality = importlib.import_module("data_quality")
connector = importlib.import_module("connector")

# Encryption key setup
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

# Main tab logic
if selected_tab == "Home":
    st.title("Unified Data Utility App")
    st.markdown("""
    Welcome to the Unified Data Utility App! This tool helps you manage and automate:
    - Data Profiling & Visualization
    - Data Lineage
    - Script Generation (STTM, DDL, ETL/ELT)
    - Data Quality Checks
                
    Use the sidebar to navigate between features.
    """)

elif selected_tab == "Data Profiling & Visualization":
    data_profiling.run_data_profiling_ui()

elif selected_tab == "Data Lineage":
    data_lineage.run_data_lineage_ui()

elif selected_tab == "Script Generator":
    script_generator.run_script_generator_ui()

elif selected_tab == "Data Quality Checks":
    data_quality.run_data_quality_ui()

# Floating Chat Panel
if show_chat:
    st.markdown("---")
    col1, col2 = st.columns([1, 3])
    with col2:
         # Free-form Chat Interface
        st.write("### Chat with AI")

        user_input = st.text_input("You:", key=f"user_input_{st.session_state.chat_input_key}")

        if st.button("Clear Conversation"):
            st.session_state.messages = [{"role": "system", "content": "Your own assistant."}]
            st.session_state.chat_input_key += 1  # Reset input box

        if user_input:
            st.session_state.messages.append({"role": "user", "content": user_input})

            try:
                response = openai.ChatCompletion.create(
                    engine='gpt-35-turbo',
                    messages=st.session_state.messages
                )
                assistant_message = response['choices'][0]['message']['content']
                st.session_state.messages.append({"role": "assistant", "content": assistant_message})
            except Exception as e:
                st.error(f"Error: {e}")

        for message in st.session_state.messages:
            if message["role"] == "user":
                st.write(f"**You:** {message['content']}")
            elif message["role"] == "assistant":
                st.write(f"**Assistant:** {message['content']}")
        st.markdown("### Choose to Know more")

        options = [
            "Option 1: Data Profiling & Visualization ",
            "Option 2: Data Lineage",
            "Option 3: Script Generation (STTM, DDL, ETL/ELT)",
            "Option 4: Data Quality Checks"
        ]

        st.write("Hello! Please select one of the following options:")
        for i, option in enumerate(options, start=1):
            st.write(f"{i}. {option}")

        user_choice = st.text_input("Type the number of your choice (e.g., 1, 2, 3, or 4):", key=f"option_input_{st.session_state.option_input_key}")

        if st.button("Clear Chat"):
            st.session_state.messages1 = []
            st.session_state.option_input_key += 1
            st.rerun()  # Force immediate refresh


        if user_choice:
            if user_choice.isdigit() and 1 <= int(user_choice) <= len(options):
                prompt = f"Selected: {options[int(user_choice) - 1]}"
                st.session_state.messages1 = [{"role": "user", "content": prompt}]

                response = openai.ChatCompletion.create(
                    deployment_id="gpt-35-turbo",
                    messages=st.session_state.messages1,
                    max_tokens=100
                )

                st.write("Response from assistant:")
                st.write(response['choices'][0]['message']['content'])

                if int(user_choice) == 3:
                    importlib.import_module("script_generator")
                    script_generator.run_script_generator_ui()
                elif int(user_choice) == 2:
                    importlib.import_module("data_lineage")
                    data_lineage.run_data_lineage_ui()
                elif int(user_choice) == 1:
                    importlib.import_module("data_profiling")
                    data_profiling.run_data_profiling_ui()
                else:
                    importlib.import_module("data_quality")
                    data_quality.run_data_quality_ui()
            else:
                st.write("Invalid choice. Please try again.")

        for message in st.session_state.messages1:
            if message["role"] == "user":
                st.write(f"**You:** {message['content']}")
            else:
                st.write(f"**Assistant:** {message['content']}")

       

# Floating Connector Panel
if show_connector:
    st.markdown("---")
    st.markdown("### Connector Panel")
    connector.run_connector_ui()