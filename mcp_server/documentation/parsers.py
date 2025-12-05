import io
import re

import pandas as pd

from mcp_server.documentation.models import (DatasetFull, NodeType,
                                             RelationshipType)


def parse_markdown_table(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # 1. Pre-process: Filter for lines that actually look like table rows
    #    (Markdown tables always contain pipe characters)
    table_lines = [line for line in lines if '|' in line]

    if not table_lines:
        print(f'No table found in {filename}')
        return None

    # Join the filtered lines back into a single string
    table_str = ''.join(table_lines)

    # 2. Parse with Pandas
    #    Using pipe separator, assuming first line is header
    df = pd.read_csv(io.StringIO(table_str), sep='|', header=0)

    # --- Cleaning Steps ---

    # A. Remove the first and last columns (empty due to outer pipes)
    #    We check column count to avoid errors on empty dataframes
    if df.shape[1] > 1:
        df = df.iloc[:, 1:-1]

    # B. Remove the separator row (e.g., ---|---)
    #    We check the first cell of the first row.
    #    Markdown separator lines typically start with dashes.
    if len(df) > 0:
        first_cell = str(df.iloc[0, 0]).strip()
        if first_cell.startswith('---'):
            df = df.iloc[1:]

    # C. Clean whitespace from headers
    df.columns = df.columns.str.strip()

    # D. Clean whitespace from data cells
    #    Only apply to object (string) columns to avoid errors on numbers
    df = df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)

    df = df.reset_index(drop=True)

    # Optionally forward fill refactored cells (dataset table)
    df = df.replace('', pd.NA).ffill()

    # Optionally explode reference_name comma-separated values (dataset table)
    if 'reference_name' in df.columns:
        df['reference_name'] = df['reference_name'].str.split(',')
        df = df.explode('reference_name')
        # Step 3: Clean up whitespace (removes the space after the comma)
        df['reference_name'] = df['reference_name'].str.strip()

    return df


def parse_datasets(md_path: str = 'documentation/data-sources.md') -> list[DatasetFull]:
    df = parse_markdown_table(md_path)

    datasets = []
    for _, row in df.iterrows():
        reference_name = (
            None if row['reference_name'] is pd.NA else row['reference_name']
        )

        readme_url = re.search(r'\[.*?\]\((.*?)\)', row['Crawler']).group(1)

        readme_path = readme_url.replace(
            'https://github.com/InternetHealthReport/internet-yellow-pages/tree/main/',
            './'
        ).replace('#readme', '/README.md')
        with open(readme_path, 'r') as f:
            readme_content = f.read()
        readme_header = readme_content.split('\n## ', 1)[0]

        dataset = DatasetFull(
            organization=row['Organization'],
            name=row['Dataset Name / Description'],
            url=row['URL'],
            readme_url=readme_url,
            reference_name=reference_name,
            readme_header=readme_header,
            readme_content=readme_content,
        )
        datasets.append(dataset)

    return datasets


def parse_node_types(md_path: str = 'documentation/node-types.md') -> list[NodeType]:
    df = parse_markdown_table(md_path)

    node_types = []
    for _, row in df.iterrows():
        node_type = NodeType(
            name=row['Node types'], description=row['Description']
        )
        node_types.append(node_type)

    return node_types


def parse_relationship_types(
    md_path: str = 'documentation/relationship-types.md',
) -> list[RelationshipType]:
    df = parse_markdown_table(md_path)

    relationship_types = []
    for _, row in df.iterrows():
        relationship_type = RelationshipType(
            name=row['Relationship'], description=row['Description']
        )
        relationship_types.append(relationship_type)

    return relationship_types
