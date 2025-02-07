import glob
import json

from neo4j import GraphDatabase
from OTXv2 import OTXv2, USER_PULSES
from tqdm import tqdm

from api_keys import OTX_API
from schema import *
from load_attack import get_aliases

DATA_DIR = '/mnt/raid1_ssd_4tb/datasets/mitre_kg/otx_pulses'

class MyOTX(OTXv2):
    def get_user_pulses(self, username, query=None):
        uri = self.create_url(USER_PULSES.format(username))

        # Setting iter to true so it returns all of them, not just the first 10
        return self.walkapi(uri, max_items=None, iter=True)

def sanitize(s):
    s = s.replace('"', "'")
    s = s.replace('\\', '[backslash]')
    return s

def build_dataset():
    otx = MyOTX(OTX_API)
    pulses = otx.get_user_pulses('AlienVault')

    for p in tqdm(pulses):
        p = otx.get_pulse_details(p['id'])
        if p.get('attack_ids'):
            with open(f"otx_pulses/{p['id']}.json", 'w+') as f:
                json.dump(p, f, indent=1)

def add_list_of_nodes(ls, nodetype, rel_type, offset=0):
    nodes = []
    edges = []

    if not ls:
        return '', '', 0

    for i,n in enumerate(ls):
        nodes.append(f'''
        MERGE (c{i+offset}:node:{nodetype} {{value: "{n}"}})
            ON CREATE SET c{i}.src = "otx"
        ''')

        edges.append(f'''
        MERGE (event) -[:{rel_type} {{src: "otx"}}]-> (c{i+offset})
        ''')

    return '\n'.join(nodes), '\n'.join(edges), offset+i+1

def add_event(event, driver, aliases):
    value = event['id']
    description = sanitize(event['name'])
    apt = event['adversary']

    # Edges to nodes
    ttps = event['attack_ids']
    countries = event['targeted_countries']
    malware = event['malware_families']
    # iocs = event['indicators'] # Maybe later

    query = f'''
        MERGE (event:node:{EVENT} {{value: "{value}"}})
            ON CREATE SET event.src="otx", event.description="{description}"
        WITH event
        MATCH  (ttp:node:{OFF_TECH}) where ttp.value in $ttps
        MERGE (event) -[:{ATTACKER_USED} {{src: "otx"}}]-> (ttp)
    '''

    offset = 0
    if countries:
        countries = [c.upper() for c in countries]
        country_nodes, country_edges, offset = add_list_of_nodes(
            countries, COUNTRY, TARGETS_COUNTRY
        )
        query += country_nodes + '\n' + country_edges

    if malware:
        malware = [m.upper() for m in malware]
        new_malware = []
        for m in malware:
            # Some weird corner cases
            if m == 'CUBA':
                new_malware.append('CUBA-RANSOMWARE')
            elif m == 'EMAIL' or m == 'URL':
                continue
            else:
                new_malware.append(m)

        malware = new_malware
        malware_nodes, malware_edges, offset = add_list_of_nodes(
            malware, f'{MALWARE}:{SOFTWARE}', ATTACKER_USED, offset=offset
        )
        query += malware_nodes + '\n' + malware_edges

    if apt and (nid := aliases.get(apt.lower())):
        query += f'''
            MERGE (apt:node:{GROUP} {{value: "{nid}"}})
                ON CREATE SET apt.src = "otx", apt.description = "{nid}"
            MERGE (event) -[:{ATTRIBUTED_TO} {{src: "otx"}}]-> (apt)
        '''

    try:
        driver.execute_query(
            query, ttps=ttps
        )
    except Exception as e:
        print(e)
        print(query)
        exit()

if __name__ == '__main__':
    driver = GraphDatabase.driver('neo4j://gemini0.ece.seas.gwu.edu/')
    aliases = get_aliases()

    events = glob.glob(DATA_DIR + '/*.json')
    for e in tqdm(events):
        with open(e, 'r') as f:
            db = json.load(f)
            add_event(db, driver, aliases)
