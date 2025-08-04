import streamlit as st
import re
import networkx as nx
import matplotlib.pyplot as plt
import tempfile
import os
import pandas as pd
from connector import get_teradata_connection, get_saved_connections

def run_data_lineage_ui():
    # Streamlit UI setup
    st.set_page_config(page_title="Teradata Lineage Explorer", layout="wide")
    st.title("Teradata Full View Lineage Explorer")

    # Show saved Teradata connections
    saved_conns = get_saved_connections("teradata")
    if saved_conns:
        selected_conn = st.selectbox("Select Teradata Connection", saved_conns)
    else:
        st.warning("⚠️ No saved Teradata connections found. Please create one in the Connector panel.")
        st.stop()

    # Fetch view definition
    def fetch_view_definition(view_name):
        try:
            connection = get_teradata_connection(selected_conn)
            if connection is None:
                st.stop()
            cursor = connection.cursor()
            cursor.execute(f"""
                SELECT RequestText
                FROM DBC.TablesV
                WHERE TableKind = 'V' AND TableName = '{view_name}';
            """)
            result = cursor.fetchone()
            if result:
                return result[0]
        except Exception as e:
            st.error(f"Error fetching view definition: {e}")
        return None

    # Fetch table metadata
    def fetch_table_metadata(database_name, table_name):
        try:
            connection = get_teradata_connection(selected_conn)
            if connection is None:
                st.stop()
            cursor = connection.cursor()
            cursor.execute(f"""
                SELECT ColumnName, ColumnFormat, ColumnType, ColumnLength, Nullable, CompressValueList
                FROM DBC.Columns
                WHERE DatabaseName = '{database_name}' AND TableName = '{table_name}';
            """)
            rows = cursor.fetchall()
            if rows:
                columns = ['ColumnName', 'ColumnFormat', 'ColumnType', 'ColumnLength', 'Nullable', 'CompressValueList']
                return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            st.error(f"Error fetching metadata for table {database_name}.{table_name}: {e}")
        return None

    # Extract dependencies
    def extract_dependencies(view_sql):
        pattern = r'\bFROM\s+([\w\.]+)|\bJOIN\s+([\w\.]+)'
        matches = re.findall(pattern, view_sql, re.IGNORECASE)
        dependencies = set()
        for match in matches:
            for item in match:
                if item:
                    dependencies.add(item.strip())
        return list(dependencies)

    # Recursive lineage builder
    def build_full_lineage(view_name, graph, visited, ddl_dict, table_metadata_dict):
        if view_name in visited:
            return
        visited.add(view_name)

        view_sql = fetch_view_definition(view_name)
        if view_sql:
            ddl_dict[view_name] = view_sql
            dependencies = extract_dependencies(view_sql)
            for dep in dependencies:
                graph.add_edge(dep, view_name)
                build_full_lineage(dep, graph, visited, ddl_dict, table_metadata_dict)
        else:
            if '.' in view_name:
                db_name, tbl_name = view_name.split('.', 1)
            else:
                db_name = "dbc"  # fallback schema
                tbl_name = view_name

            metadata = fetch_table_metadata(db_name, tbl_name)
            if metadata is not None and not metadata.empty:
                table_metadata_dict[view_name] = metadata

    # Visualize graph
    def visualize_graph(graph):
        fig, ax = plt.subplots(figsize=(10, 8))
        pos = nx.spring_layout(graph)
        nx.draw(graph, pos, with_labels=True, node_color='lightblue', edge_color='gray',
                node_size=2000, font_size=10, arrows=True, ax=ax)
        ax.set_title("Full View Lineage Graph")

        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
        plt.savefig(temp_file.name)
        plt.close()
        return temp_file.name

    # Input for view name
    view_name = st.text_input("Enter the view name (e.g., sales_summary):")
    generate_btn = st.button("🔍 Generate Lineage")

    if generate_btn and view_name:
        G = nx.DiGraph()
        visited = set()
        ddl_dict = {}
        table_metadata_dict = {}

        build_full_lineage(view_name, G, visited, ddl_dict, table_metadata_dict)

        if ddl_dict or table_metadata_dict:
            st.subheader("Lineage Graph")
            graph_path = visualize_graph(G)
            st.image(graph_path)
            os.unlink(graph_path)

            st.subheader("View Definitions")
            for name in reversed(list(ddl_dict.keys())):
                with st.expander(f"View: {name}"):
                    st.code(ddl_dict[name], language='sql')

            st.subheader("Table Metadata")
            for table_name, metadata_df in table_metadata_dict.items():
                with st.expander(f"Table: {table_name}"):
                    st.dataframe(metadata_df, use_container_width=True)
        else:
            st.warning("No view or table definitions found.")
