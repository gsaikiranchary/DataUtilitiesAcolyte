import streamlit as st
import pandas as pd
import numpy as np
import re
from sklearn.impute import SimpleImputer
import matplotlib.pyplot as plt
from scipy.stats import skew, kurtosis, zscore

# Import shared connection functions
from connector import (
    get_teradata_connection,
    get_azure_sql_connection,
    get_databricks_catalog,
    get_saved_connections
)

def run_data_profiling_ui():
    st.title("🧮 Data Profiling & Visualization")

    def fetch_metadata_from_csv(df):
        metadata = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            if dtype == 'object':
                inferred_type = 'VARCHAR(100)'
            elif 'float' in dtype:
                inferred_type = 'DECIMAL(18,4)'
            elif 'int' in dtype:
                inferred_type = 'INTEGER'
            elif 'datetime' in dtype:
                inferred_type = 'DATE'
            elif 'bool' in dtype:
                inferred_type = 'BOOLEAN'
            else:
                inferred_type = dtype.upper()
            nullable = 'NOT NULL' if df[col].isnull().sum() == 0 else 'NULL'
            metadata.append({
                'ColumnName': col,
                'DataType': inferred_type,
                'Nullable': nullable
            })
        return pd.DataFrame(metadata)

    def fetch_data_from_teradata(table_name, conn_name):
        con = get_teradata_connection(conn_name)
        if con is None:
            st.stop()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, con)
        con.close()
        return df

    def fetch_data_from_azure_sql(table_name, conn_name):
        con = get_azure_sql_connection(conn_name)
        if con is None:
            st.stop()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, con)
        con.close()
        return df

    def fetch_data_from_databricks(conn_name):
        catalog_data = get_databricks_catalog(conn_name)
        if catalog_data:
            st.json(catalog_data)
            st.stop()
        else:
            st.warning("No catalog data found or credentials missing.")
            st.stop()

    data_source = st.radio("Choose Data Source", ["Upload CSV", "Teradata Table", "Azure SQL DB", "Databricks Catalog"])

    df = None

    if data_source == "Upload CSV":
        st.info("Please upload a CSV file to proceed.")
        uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])
        if uploaded_file:
            df = pd.read_csv(uploaded_file)

    else:
        source_key_map = {
            "Teradata Table": "teradata",
            "Azure SQL DB": "azuresql",
            "Databricks Catalog": "databricks"
        }
        source_key = source_key_map.get(data_source, "")
        saved_conns = get_saved_connections(source_key)
        if not saved_conns:
            st.warning(f"No saved {data_source} connections found. Please create one in the Connector panel.")
            st.stop()
        selected_conn = st.selectbox(f"Select {data_source} Connection", saved_conns)

        table_name = st.text_input(f"Enter {data_source} Table Name")
        if table_name:
            try:
                if data_source == "Teradata Table":
                    df = fetch_data_from_teradata(table_name, selected_conn)
                elif data_source == "Azure SQL DB":
                    df = fetch_data_from_azure_sql(table_name, selected_conn)
                elif data_source == "Databricks Catalog":
                    fetch_data_from_databricks(selected_conn)
            except Exception as e:
                st.error(f"Error fetching data: {e}")

    if df is not None:
        st.subheader("📊 Original Data Preview")
        st.markdown("""
    **📊 Original Data Preview**
    - - Displays the first few rows of the dataset using `st.dataframe`.
    - - Helps verify the structure and content of the loaded data.
    """)
        st.dataframe(df.head())

        st.subheader("🗾 Inferred Metadata")
        st.markdown("""
**Inferred Metadata**
- Detects data types (e.g., INTEGER, DATE, VARCHAR) based on column values.
- Flags columns as NULL or NOT NULL depending on missing values.
- Helps understand the structure and quality of the dataset.
""")
        metadata_df = fetch_metadata_from_csv(df)
        st.dataframe(metadata_df)

        conversion_summary = {}

        st.subheader("📅 Date Conversion")
        st.markdown("""
    **📅 Date Conversion**
    - - Converts columns with 'date' in their name to datetime format.
    - - Uses `pd.to_datetime` with error coercion.
    - - Summarizes converted columns in JSON format.
    """)
        for col in df.columns:
            if "date" in col.lower():
                df[col] = pd.to_datetime(df[col], errors='coerce')
                conversion_summary[col] = "Converted to datetime"
        st.json({k: v for k, v in conversion_summary.items() if v == "Converted to datetime"})

        st.subheader("🔘 Boolean Conversion")
        st.markdown("""
    **🔘 Boolean Conversion**
    - - Detects columns with values like 'yes', 'no', 'true', 'false', '0', '1'.
    - - Converts them to boolean type using mapping logic.
    - - Displays conversion summary in JSON format.
    """)
        for col in df.select_dtypes(include='object').columns:
            unique_vals = df[col].dropna().unique()
            if set(map(str.lower, map(str, unique_vals))).issubset({'yes', 'no', 'true', 'false', '0', '1'}):
                df[col] = df[col].map(lambda x: str(x).strip().lower() in ['yes', 'true', '1'])
                conversion_summary[col] = "Converted to boolean"
        st.json({k: v for k, v in conversion_summary.items() if v == "Converted to boolean"})

        st.subheader("🏷️ Category Conversion")
        st.markdown("""
    **🏷️ Category Conversion**
    - - Converts object-type columns with ≤ 20 unique values to categorical type.
    - - Skips columns already converted to boolean.
    - - Updates conversion summary in JSON format.
    """)
        for col in df.select_dtypes(include='object').columns:
            if df[col].nunique() <= 20 and col not in conversion_summary:
                df[col] = df[col].astype('category')
                conversion_summary[col] = "Converted to category"
        st.json({k: v for k, v in conversion_summary.items() if v == "Converted to category"})

        st.subheader("🔢 Symbolic Numeric Parsing")
        st.markdown("""
    **🔢 Symbolic Numeric Parsing**
    - - Identifies object columns with symbols like $, %, ,.
    - - Removes symbols using regex and converts to numeric.
    - - Adds conversion details to summary in JSON format.
    """)
        for col in df.columns:
            if df[col].dtype == 'object':
                sample_vals = df[col].dropna().astype(str).head(10)
                if sample_vals.apply(lambda x: bool(re.search(r'[$%,]', x))).any():
                    df[col] = df[col].replace('[$%,]', '', regex=True)
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    conversion_summary[col] = "Parsed numeric values with symbols"
        st.json({k: v for k, v in conversion_summary.items() if v == "Parsed numeric values with symbols"})

        st.subheader("🧮 Missing Value Imputation")
        st.markdown("""
    **🧮 Missing Value Imputation**
    - - Applies mean imputation to numeric columns using `SimpleImputer`.
    - - Replaces missing values with column mean.
    - - Ensures completeness of numeric data.
    """)
        numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
        imputer = SimpleImputer(strategy='mean')
        df[numeric_columns] = imputer.fit_transform(df[numeric_columns])
        st.write("Missing values in numeric columns have been imputed with mean.")

        st.subheader("🧹 Duplicate Removal")
        st.markdown("""
    **🧹 Duplicate Removal**
    - - Removes duplicate rows using `drop_duplicates`.
    - - Displays count of removed duplicates.
    """)
        before = len(df)
        df = df.drop_duplicates()
        after = len(df)
        st.write(f"Removed {before - after} duplicate rows.")

        st.subheader("🔄 Conversion Summary")
        st.json(conversion_summary)

        st.subheader("📈 Descriptive Statistics")
        st.markdown("""
    **📈 Descriptive Statistics**
    - - Displays statistical summary using `df.describe()`.
    - - Includes count, mean, std, min, max, and quartiles.
    """)
        st.dataframe(df.describe())

        st.subheader("📐 Skewness and Kurtosis")
        st.markdown("""
    **Skewness and Kurtosis**
    - - Skewness shows how symmetric the data distribution is.
    - - Kurtosis indicates the presence of outliers and the sharpness of the peak.
    """)
        numerical_cols = numeric_columns.tolist()
        for col in numerical_cols:
            col_skew = skew(df[col])
            col_kurt = kurtosis(df[col])
            st.write(f"{col}: Skewness = {col_skew:.2f}, Kurtosis = {col_kurt:.2f}")

        st.subheader("📊 Histograms")
        st.markdown("""
    **📊 Histograms**
    - - Plots distribution of each numeric column using histograms.
    - - Reveals modality, spread, and skewness visually.
    - - Uses `matplotlib` for rendering histograms.
    """)
        for col in numerical_cols:
            fig, ax = plt.subplots()
            df[col].dropna().hist(bins=20, ax=ax, color='skyblue', edgecolor='black')
            ax.set_title(f'Distribution of {col}')
            st.pyplot(fig)

        st.subheader("📉 Scatter Plots")
        st.markdown("""
    **📉 Scatter Plots**
    - - Generates scatter plots for all pairs of numeric columns.
    - - Helps visualize relationships and correlations between variables.
    - - Useful for detecting linear or non-linear patterns.
    """)
        for i in range(len(numerical_cols)):
            for j in range(i + 1, len(numerical_cols)):
                fig, ax = plt.subplots()
                ax.scatter(df[numerical_cols[i]], df[numerical_cols[j]], alpha=0.5)
                ax.set_xlabel(numerical_cols[i])
                ax.set_ylabel(numerical_cols[j])
                ax.set_title(f'{numerical_cols[i]} vs {numerical_cols[j]}')
                st.pyplot(fig)

        st.subheader("🚨 Outlier Detection")
        st.markdown("""
    **🚨 Outlier Detection**
    - - Detects outliers using Z-score and IQR methods.
    - - Z-score method flags values with |z| > 3.
    - - IQR method flags values outside 1.5×IQR range.
    - - Displays count of outliers per column for both methods.
    """)
        z_scores = df[numerical_cols].apply(zscore)
        z_outliers = (np.abs(z_scores) > 3)
        for col in numerical_cols:
            st.write(f"{col}: Z-score Outliers = {z_outliers[col].sum()}")

        for col in numerical_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            outliers = df[(df[col] < Q1 - 1.5 * IQR) | (df[col] > Q3 + 1.5 * IQR)]
            st.write(f"{col}: IQR Outliers = {len(outliers)}")

        st.subheader("🔍 Referential Integrity Checks")
        st.markdown("""
    **🔍 Referential Integrity Checks**
    - - Checks for duplicate `Sale_ID`s to ensure uniqueness.
    - - Analyzes `Agent_ID`s for multiple sales handling.
    - - Displays counts of unique agents and those with multiple sales.
    """)
        if "Sale_ID" in df.columns:
            st.write(f"Duplicate Sale_IDs: {df['Sale_ID'].duplicated().sum()}")
        if "Agent_ID" in df.columns:
            agent_sales_counts = df['Agent_ID'].value_counts()
            st.write(f"Unique Agent_IDs: {df['Agent_ID'].nunique()}")
            st.write(f"Agents handling multiple sales: {agent_sales_counts[agent_sales_counts > 1].count()}")

