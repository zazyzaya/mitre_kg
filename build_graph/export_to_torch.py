from neo4j import GraphDatabase, Driver
import torch
from torch_geometric.data import Data
from tqdm import tqdm

LABELS = {
    'ARTIFACT': 0,
    'CAMPAIGN': 1,
    'COUNTRY': 2,
    'DEFENSIVE_TACTIC': 3,
    'DEFENSIVE_TECHNIQUE': 4,
    'EVENT': 5,
    'OFFENSIVE_TACTIC': 6,
    'OFFENSIVE_TECHNIQUE': 7,
    'SOFTWARE': 8,
    'THREAT_GROUP': 9,
    'malware': 10,
    'tool': 11
}
IGNORE = ['node']

def build_nodes(driver: Driver):
    nmap = dict()
    names = []
    x = []

    q = 'MATCH (n) return n.value as value, LABELS(n) as labels, n.description as desc'
    resp = driver.execute_query(q)

    for i,r in tqdm(enumerate(resp.records)):
        val = r.get('value')
        labels = r.get('labels')
        desc = r.get('desc')

        nmap[val] = i
        names.append(desc)
        vector = [0.] * len(LABELS)
        for l in labels:
            if l not in IGNORE:
                vector[LABELS[l]] = 1.

        x.append(vector)

    return torch.tensor(x), nmap, names

def build_edges(driver: Driver, nmap: dict):
    q = 'MATCH (u) -- (v) return u.value as u, v.value as v'
    resp = driver.execute_query(q)

    src,dst = [],[]
    for r in tqdm(resp.records):
        u = r.get('u')
        v = r.get('v')

        src.append(nmap[u])
        dst.append(nmap[v])

    return torch.tensor([src,dst])


if __name__ == '__main__':
    driver = GraphDatabase.driver('neo4j://gemini0.ece.seas.gwu.edu/')
    try:
        x, nmap, names = build_nodes(driver)
        ei = build_edges(driver, nmap)
    finally:
        driver.close()

    torch.save(
        Data(
            x=x, edge_index=ei, node_names=names
        ),
        'kg.pt'
    )