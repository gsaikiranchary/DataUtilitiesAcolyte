import streamlit as st
import pandas as pd
from connector import (
    get_teradata_connection,
    get_azure_sql_connection,
    get_databricks_catalog,
    get_saved_connections
)

def run_data_quality_ui():
    st.title("Data Quality Checks")

    data_source = st.radio("Choose Data Source", ["Upload CSV", "Teradata Table", "Azure SQL DB", "Databricks Catalog"])
    df = None

    if data_source == "Upload CSV":
        uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])
        if uploaded_file:
            df = pd.read_csv(uploaded_file)
        else:
            st.stop()
    else:
        source_key_map = {
            "Teradata Table": "teradata",
            "Azure SQL DB": "azuresql",
            "Databricks Catalog": "databricks"
        }
        source_key = source_key_map.get(data_source, "")
        saved_conns = get_saved_connections(source_key)
        if not saved_conns:
            st.warning(f"No saved {data_source} connections found. Please create one in the Connector panel from Home page.")
            st.stop()
        selected_conn = st.selectbox(f"Select {data_source} Connection", saved_conns)
        table_name = st.text_input(f"Enter {data_source} Table Name")
        if table_name:
            try:
                if data_source == "Teradata Table":
                    con = get_teradata_connection(selected_conn)
                    if con is None:
                        st.stop()
                    query = f"SELECT * FROM {table_name}"
                    df = pd.read_sql(query, con)
                    con.close()
                elif data_source == "Azure SQL DB":
                    con = get_azure_sql_connection(selected_conn)
                    if con is None:
                        st.stop()
                    query = f"SELECT * FROM {table_name}"
                    df = pd.read_sql(query, con)
                    con.close()
                elif data_source == "Databricks Catalog":
                    catalog_data = get_databricks_catalog(selected_conn)
                    if catalog_data:
                        st.json(catalog_data)
                        st.info(f"Fetched metadata for table: {table_name}")
                    else:
                        st.warning("No catalog data found or credentials missing.")
                    st.stop()
            except Exception as e:
                st.error(f"Error fetching data: {e}")
                st.stop()
        else:
            st.stop()

    st.write("### Preview of Data")
    st.dataframe(df.head())

    # Move test configuration to main UI
    st.markdown("## Configure Data Quality Checks")

    null_check_cols = st.multiselect("Select columns for Null Check", df.columns.tolist())

    st.markdown("### Data Type Validation")
    type_check_cols = st.multiselect("Select columns for Type Check", df.columns.tolist())
    expected_types = {}
    for col in type_check_cols:
        expected_types[col] = st.selectbox(f"Expected type for {col}", ["int", "float", "str", "date", "datetime", "timestamp", "bool"], key=f"type_{col}")

    st.markdown("### Range Validation")
    range_check_cols = st.multiselect("Select columns for Range Check", df.columns.tolist())
    range_rules = {}
    for col in range_check_cols:
        min_val = st.number_input(f"Min value for {col}", key=f"min_{col}")
        max_val = st.number_input(f"Max value for {col}", key=f"max_{col}")
        range_rules[col] = (min_val, max_val)

    def check_nulls(df, columns):
        return df[columns].isnull().sum()

    def check_duplicates(df):
        dupdf = df[df.duplicated(keep=False)].groupby(list(df.columns)).size().reset_index(name='dup_count')
        return dupdf

    def check_data_types(df, expected_types):
        mismatches = {}
        for col, expected_type in expected_types.items():
            if col in df.columns:
                actual_type = df[col].dropna().map(type).mode()[0].__name__
                if actual_type != expected_type:
                    mismatches[col] = f"Expected {expected_type}, Found {actual_type}"
        return mismatches

    def check_ranges(df, range_rules):
        violations = {}
        for col, (min_val, max_val) in range_rules.items():
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    invalid = df[(df[col] < min_val) | (df[col] > max_val)]
                    if not invalid.empty:
                        violations[col] = invalid[[col]]
                except Exception as e:
                    violations[col] = f"Error: {e}"
        return violations

    if st.button("Run Data Quality Checks"):
        st.subheader("Null Value Check")
        if null_check_cols:
            nulls = check_nulls(df, null_check_cols)
            st.write(nulls)
        else:
            st.write("No columns selected for null check.")

        st.subheader("Duplicate Records")
        duplicates = check_duplicates(df)
        if not duplicates.empty:
            st.write(duplicates)
        else:
            st.write("No duplicate records found.")

        st.subheader("Data Type Validation")
        if expected_types:
            type_mismatches = check_data_types(df, expected_types)
            if type_mismatches:
                st.write(type_mismatches)
            else:
                st.write("All types match expected values.")
        else:
            st.write("No columns selected for type validation.")

        st.subheader("Range Validation")
        if range_rules:
            range_violations = check_ranges(df, range_rules)
            if range_violations:
                for col, result in range_violations.items():
                    st.write(f"Violations in {col}:", result)
            else:
                st.write("All values within specified ranges.")
        else:
            st.write("No columns selected for range validation.")
