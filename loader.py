import json
from neo4j import GraphDatabase
from tqdm import tqdm

# Node types
OFF_TECH = 'OFFENSIVE_TECHNIQUE'
DEF_TECH = 'DEFENSIVE_TECHNIQUE'
DEF_TAC = 'DEFENSIVE_TACTIC'
OFF_TAC = 'OFFENSIVE_TACTIC'
ARTIFACT = 'ARTIFACT'

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
    off_tactic_rel = blob['off_tactic_rel_label']['value']

    # Only add edges for fine-grained nodes
    if primary_node == def_tech_parent:
        return [],[]

    nodes = [
        f'(def_{id}:node:{DEF_TECH}:{sanitize(def_tech_parent)} {{value: "{def_tech_parent}: {primary_node}"}})',
        f'(art_{id}:node:{ARTIFACT}:{sanitize(artifact_class)} {{value: "{artifact_class}: {artifact}"}})',
        f'(off_{id}:node:{OFF_TECH}:{sanitize(off_tech_parent)} {{value: "{off_tech_parent}: {off_tech}", attack_id: "{off_tech_id}"}})',
        f'(dt_{id}:node:{DEF_TAC} {{value: "{def_tactic}"}})',
        f'(ot_{id}:node:{OFF_TAC} {{value: "{off_tactic}"}})'
    ]

    edges = [
        f'(def_{id}) -[:{sanitize(def_artifact_rel)}]- (art_{id})',
        f'(off_{id}) -[:{sanitize(off_artifact_rel)}]- (art_{id})',
        f'(dt_{id}) -[:{sanitize(def_tactic_rel)}]- (def_{id})',
        f'(ot_{id}) -[:{sanitize(off_tactic_rel)}]- (off_{id})'
    ]

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

    nodes,edges = [],[]
    for i in tqdm(range(len(db))):
        n,e = blob_to_edges(db[i], i % BUFF_SIZE)
        nodes += n; edges += e

        if i % BUFF_SIZE and i and edges:
            n_query = '\n'.join([f'MERGE {n}' for n in nodes])
            e_query = '\n'.join([f'MERGE {e}' for e in edges])
            query = n_query + '\n' + e_query + ';'

            driver.execute_query(query, database_='neo4j')
            nodes,edges = [],[]

populate_db()