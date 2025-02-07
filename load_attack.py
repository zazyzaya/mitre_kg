from neo4j import GraphDatabase
import numpy as np
import pandas as pd
from tqdm import tqdm

from load_d3fense import sanitize
from schema import *

EXCEL_FILE = '/mnt/raid1_ssd_4tb/datasets/mitre_kg/enterprise-attack-v16.1.xlsx'
NODE_SHEETS = ['techniques', 'tactics', 'software', 'groups', 'campaigns']
REL_SHEET = 'relationships'

sheet_to_type = {
    'techniques': OFF_TECH,
    'tactics': OFF_TAC,
    'software': SOFTWARE,
    'groups': GROUP,
    'campaigns': CAMPAIGN,
}

def loads_nodes(driver):
    for sn in NODE_SHEETS:
        nodes = []
        df = pd.read_excel(EXCEL_FILE, sheet_name=sn)

        for row in df.itertuples():
            labels = f'node:{sheet_to_type[sn]}'
            uuid = row.ID.upper()
            name = row.name
            idx = row.Index

            if sn == 'techniques':
                name = name.split(': ')[-1]

            if sn == 'software':
                # Tool or malware
                labels += f':{row.type}'

            nodes.append(
                (f'(n{idx}:{labels} {{value: "{uuid}"}})', name, idx)
            )

        print(f"Inserting {sn} nodes!")

        q = [
            f'MERGE {node} ON CREATE SET n{idx}.description = "{name}", n{idx}.src = "attack"'
            for node,name,idx in nodes
        ]
        q = '\n'.join(q)
        q += ';'
        driver.execute_query(q)


def reads_rels(driver):
    SRC = 1; REL = 5;  DST = 6
    df = pd.read_excel(EXCEL_FILE, sheet_name=REL_SHEET)

    edges = []
    for row in df.itertuples():
        src = row[SRC]
        rel = row[REL]
        dst = row[DST]

        if src and dst:
            edges.append((src,rel,dst))

    for i,e in tqdm(enumerate(edges)):
        q = f'''
        MATCH (src_{i}) where src_{i}.value = "{e[0]}"
        MATCH (dst_{i}) where dst_{i}.value = "{e[2]}"
        CREATE (src_{i}) -[:{sanitize(e[1])} {{src: "attack"}}]-> (dst_{i})
        '''
        driver.execute_query(q)

def get_aliases():
    alias_dict = dict()
    df = pd.read_excel(EXCEL_FILE, sheet_name='groups')
    for row in df.itertuples():
        nid = row.ID
        aliases = [row.name.lower()]

        if isinstance(row, str):
            others = row[11]
            others = others.split(', ')
            others = [o.lower() for o in others]
        else:
            others = []

        aliases += others
        for alias in aliases:
            alias_dict[alias] = nid

    return alias_dict

if __name__ == '__main__':
    driver = GraphDatabase.driver('neo4j://gemini0.ece.seas.gwu.edu/')
    try:
        loads_nodes(driver)
        reads_rels(driver)
    finally:
        driver.close()