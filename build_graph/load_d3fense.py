import json
from neo4j import GraphDatabase
from tqdm import tqdm

from schema import DEF_TAC, DEF_TECH, OFF_TAC, OFF_TECH, ARTIFACT, PARENT

BUFF_SIZE = 10

JSON_FILE = '/mnt/raid1_ssd_4tb/datasets/mitre_kg/d3fend-full-mappings.json'

def sanitize(s):
    s = s.replace(" ", '')
    s = s.replace('-', '_')
    s = s.replace('/', '')
    return s

def blob_to_edges(blob, id):
    primary_node = blob['query_def_tech_label']['value']
    def_tech_parent = blob['top_def_tech_label']['value']
    def_tactic = blob['def_tactic_label']['value']
    def_tactic_rel = blob['def_tactic_rel_label']['value']
    def_tech = blob['def_tech_label']['value']   # Not used?
    def_artifact_rel = blob['def_artifact_rel_label']['value']
    artifact_class = blob['def_artifact_label']['value']
    artifact = blob['off_artifact_label']['value'] # Always(?) more fine grained than above
    off_artifact_rel = blob['off_artifact_rel_label']['value']
    off_tech = blob['off_tech_label']['value']
    off_tech_id = blob['off_tech_id']['value']
    off_tech_parent = blob['off_tech_parent_label']['value']
    off_tactic = blob['off_tactic_label']['value']
    off_tactic_id = blob['off_tactic']['value'].split("#")[-1]
    off_tactic_rel = blob['off_tactic_rel_label']['value']

    # Only add edges for fine-grained nodes
    if primary_node == def_tech_parent:
        return [],[]

    nodes = [
        f'(def_{id}:node:{DEF_TECH} {{value: "{primary_node.upper()}", src: "d3fense"}})',
        f'(art_{id}:node:{ARTIFACT} {{value: "{artifact.upper()}", src: "d3fense"}})',
        f'(off_{id}:node:{OFF_TECH} {{value: "{off_tech_id.upper()}", description: "{off_tech}", src: "d3fense"}})',
        f'(dt_{id}:node:{DEF_TAC} {{value: "{def_tactic.upper()}", src: "d3fense"}})',
        f'(ot_{id}:node:{OFF_TAC} {{value: "{off_tactic_id.upper()}", description: "{off_tactic}", src: "d3fense"}})'
    ]

    edges = [
        f'(def_{id}) -[:{sanitize(def_artifact_rel)} {{src: "d3fense"}}]- (art_{id})',
        f'(off_{id}) -[:{sanitize(off_artifact_rel)} {{src: "d3fense"}}]- (art_{id})',
        f'(dt_{id}) <-[:{sanitize(def_tactic_rel)} {{src: "d3fense"}}]- (def_{id})',
        f'(ot_{id}) <-[:{sanitize(off_tactic_rel)} {{src: "d3fense"}}]- (off_{id})'
    ]

    if def_tech_parent != primary_node:
        nodes.append(f'(dt_parent_{id}:node:{DEF_TECH} {{value: "{def_tech_parent.upper()}", src: "d3fense"}})')
        edges.append(f'(dt_parent_{id}) -[:{PARENT} {{src: "d3fense"}}]-> (def_{id})')

    if off_tech_parent != off_tech:
        if '.' in off_tech_id:
            nodes.append(f'(ot_parent_{id}:node:{OFF_TECH} {{value: "{off_tech_id.split(".")[0].upper()}", description: "{off_tech_parent}", src: "d3fense"}})')
        else:
            nodes.append(f'(ot_parent_{id}:node:{OFF_TECH} {{value: "{off_tech_parent.upper()}", top_level: "True", src: "d3fense"}})')

        edges.append(f'(ot_parent_{id}) -[:{PARENT} {{src: "d3fense"}}]-> (off_{id})')
    if artifact_class != artifact:
        nodes.append(f'(art_parent_{id}:node:{ARTIFACT} {{value: "{artifact_class.upper()}", src: "d3fense"}})')
        edges.append(f'(art_parent_{id}) -[:{PARENT} {{src: "d3fense"}}]-> (art_{id})')

    return nodes,edges

def populate_db():
    with open(JSON_FILE, 'r') as f:
        db = json.load(f)['results']['bindings']

    # Set label to be primary key for all nodes
    driver = GraphDatabase.driver('neo4j://gemini0.ece.seas.gwu.edu/')
    queries = [
        'CREATE CONSTRAINT uq_value IF NOT EXISTS FOR (n:node) REQUIRE n.value IS UNIQUE',
        #'CREATE CONSTRAINT value_exists FOR (n:node) REQUIRE n.value IS NOT NULL' # Not allowed without paying. Lame
    ]
    [driver.execute_query(q, database_='neo4j') for q in queries]

    try:
        nodes,edges = [],[]
        for i in tqdm(range(len(db))):
            n,e = blob_to_edges(db[i], i % BUFF_SIZE)
            nodes += n; edges += e

            if (i % BUFF_SIZE == BUFF_SIZE-1) and edges:
                n_query = '\n'.join([f'MERGE {n}' for n in nodes])
                e_query = '\n'.join([f'MERGE {e}' for e in edges])
                query = n_query + '\n' + e_query + ';'

                driver.execute_query(query, database_='neo4j')
                nodes,edges = [],[]

        if edges:
            n_query = '\n'.join([f'MERGE {n}' for n in nodes])
            e_query = '\n'.join([f'MERGE {e}' for e in edges])
            query = n_query + '\n' + e_query + ';'

            driver.execute_query(query, database_='neo4j')
            nodes,edges = [],[]
    finally:
        driver.close()

if __name__ == '__main__':
    populate_db()