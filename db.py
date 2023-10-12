from collections import defaultdict
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
import uuid

class GraphDBDriver:
  def __init__(self, uri: str, user: str, password: str):
    self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
  def close(self):
    self.driver.close()

  def verify_connectivity(self):
    try:
      self.driver.verify_connectivity()
    except Neo4jError as err:
      print(err.code)
      print(err.message)
      raise ValueError("Cannot connect to DB.")


def create_author(tx, author_details):
  return tx.run(
    """
      MERGE (p:Person {pid: $pid})
        ON CREATE
          SET p.name = $name
          SET p.aliases = $aliases
          SET p.affiliations = $affiliations
    """,
    pid=author_details["pid"],
    name=author_details["name"],
    aliases=author_details["aliases"],
    affiliations=author_details["affiliations"]
  )
  

def find_author(tx, author_pid):
  res = tx.run(
    """
      MATCH (p:Person {pid:$author_pid})
      RETURN p.pid AS pid
    """,
    author_pid=author_pid
  ).single()
  
  if res == None:
    return None
  return res.value("pid")


def find_authors(tx):
  res = tx.run(
    """
      MATCH (p:Person)
      RETURN p.pid AS pid
    """
  )
  return res.value("pid")


def create_relation(tx, source_pid, target_pid, publication_count):
  return tx.run(
    """
      MATCH (p:Person {pid: $source_pid})
      MATCH (q:Person {pid: $target_pid})
      MERGE (p)-[r:Coauthored_with {count: $publication_count}]->(q)
      MERGE (p)<-[s:Coauthored_with {count: $publication_count}]-(q)
    """,
    source_pid=source_pid,
    target_pid=target_pid,
    publication_count=publication_count
  )


def compute_squared_relations(tx, author_pid: str):
  res = tx.run(
    """
      MATCH (p:Person {pid:$author_pid})-[r:Coauthored_with]->(q:Person)-[s:Coauthored_with]->(t:Person)
      where not t.pid = p.pid 
      RETURN p.pid AS source_pid, t.pid AS target_pid, r.count AS count1, s.count AS count2
    """,
    author_pid=author_pid
    )
  
  squared_edges = defaultdict(lambda : [0,0])
  
  for record in res:
    source_pid = record["source_pid"]
    target_pid = record["target_pid"]
    count1 = record["count1"]
    count2 = record["count2"]
    
    squared_edges[source_pid + "_" + target_pid][0] += count1
    squared_edges[source_pid + "_" + target_pid][1] += count2
  return squared_edges


def create_squared_relation(tx, source_pid: str, target_pid: str, count: int):
  return tx.run(
    """
      MATCH (p:Person {pid: $source_pid})
      MATCH (q:Person {pid: $target_pid})
      MERGE (p)-[:Squared {count:$count}]->(q)
    """,
    source_pid=source_pid,
    target_pid=target_pid,
    count=count,
  )
  
  
def find_coauthor_relations(tx):
  res = tx.run(
    """
      MATCH (p:Person)-[r:Coauthored_with]->(q:Person)
      RETURN p as source, q as target, r.count as count
    """
  )
  return list(res)


def find_squared_relations(tx):
  res = tx.run(
    """
      MATCH (p:Person)-[r:Squared]->(q:Person)
      RETURN p as source, q as target, r.count as count
    """
  )
  return list(res)


def find_all_relations(tx):
  res = tx.run(
    """
      MATCH (p:Person)-[r]->(q:Person)
      RETURN p as source, q as target, r.count as count
    """
  )
  return list(res)


def make_author(tx, author_details):
  return tx.run(
    """
      MERGE (a:Author {name: $name})
        ON CREATE
        SET a.aid = $aid
        SET a.affiliations = $affiliations
    """,
    aid=author_details["aid"],
    name=author_details["name"],
    affiliations=author_details["affiliations"]
  )


def get_author_by_name(tx, a_name):
  res = tx.run(
    """
      MATCH (a:Author {name:$a_name})
      RETURN a
    """,
    a_name=a_name
  ).single()
  
  if res == None:
    return None
  return res


def update_author_alex_details_by_id(tx, aid, alex_id=None, alex_name=None, alex_institute=None, alex_institute_id=None):
  res = tx.run(
    """
      MATCH (a:Author {aid:$aid})
      SET a.alex_id = $alex_id
      SET a.alex_name = $alex_name
      SET a.alex_institute = $alex_institute
      SET a.alex_institute_id = $alex_institute_id
      RETURN a
    """,
    aid=aid,
    alex_id=alex_id, 
    alex_name=alex_name, 
    alex_institute=alex_institute,
    alex_institute_id=alex_institute_id
  )
  print(res)

  return res


def update_author_dblp_info_by_id(tx, aid, dblp_id=None):
  res = tx.run(
    """
      MATCH (a:Author {aid:$aid})
      SET a.dblp_id=$dblp_id
      RETURN a
    """,
    aid=aid,
    dblp_id=dblp_id
  )
  return res


def update_creator_dblp_info_by_id(tx, aid, dblp_id=None):
  res = tx.run(
    """
      MATCH (a:Creator {aid:$aid})
      SET a.dblp_id=$dblp_id
      RETURN a
    """,
    aid=aid,
    dblp_id=dblp_id
  )
  return res

def get_alex_unfound_authors(tx):
  res = tx.run(
      """
        MATCH (n:Author)
        WHERE n.alex_id IS NULL
        RETURN n
      """
    )
  return list(res)


def get_all_authors(tx):
  res = tx.run(
    """
      MATCH (n: Author)
      RETURN n.dblp_id AS dblp_id
    """
  )
  return res.value("dblp_id")

def get_all_creators(tx):
  res = tx.run(
    """
      MATCH (n:Creator)
      RETURN n.pid as pid
    """
  )
  return res.value("pid")
def get_author_by_dblp_id(tx, author_dblp_id):
  res = tx.run(
    """
      MATCH (a:Author {dblp_id:$author_pid})
      RETURN a.dblp_id AS dblp_id
    """,
    author_pid=author_dblp_id
  ).single()
  
  if res == None:
    return None
  return res.value("dblp_id")


def get_creator_by_dblp_id(tx, author_dblp_id):
  res = tx.run(
    """
      MATCH (a:Creator {pid:$author_pid})
      RETURN a.pid AS pid
    """,
    author_pid=author_dblp_id
  ).single()

  if res == None:
    return None
  return res.value("pid")

def get_authors_not_found_on_dblp(tx):
  res = tx.run(
    """
      MATCH (n: Author)
      RETURN n
    """
  )

  return list(res)

def get_creators(tx):
  res = tx.run(
    """
      MATCH (n: Creator)
      RETURN n
      SKIP 400
      LIMIT 25
    """
  )

  return list(res)


def get_wrong_dblp_authors(tx):
  res = tx.run(
    """
      MATCH (n:Author)
      RETURN n
    """
    # """
    # MATCH (n:Author)
    # WHERE NOT (n:Author)-[:coauthored_with]->(:Author)
    # RETURN n
    # """
  )

  return list(res)

def make_relation(tx, source_pid, target_pid, publication_count):
  return tx.run(
    """
      MATCH (p:Author {dblp_id: $source_pid})
      MATCH (q:Author {dblp_id: $target_pid})
      MERGE (p)-[r:coauthored_with {count: $publication_count}]->(q)
      MERGE (p)<-[s:coauthored_with {count: $publication_count}]-(q)
    """,
    source_pid=source_pid,
    target_pid=target_pid,
    publication_count=publication_count
  )


def make_creator_relation(tx, source_pid, target_pid, publication_count):
  return tx.run(
    """
      MATCH (p:Creator {pid: $source_pid})
      MATCH (q:Creator {pid: $target_pid})
      MERGE (p)-[r:coauthored {count: $publication_count}]->(q)
      MERGE (p)<-[s:coauthored {count: $publication_count}]-(q)
    """,
    source_pid=source_pid,
    target_pid=target_pid,
    publication_count=publication_count
  )

def get_coauthor_relations(tx):
  res = tx.run(
    """
      MATCH (p:Author)-[r:coauthored_with]->(q:Author)
      RETURN p as source, q as target, r.count as count
    """
  )
  return list(res)


def get_creator_relations(tx):
  res = tx.run(
    """
      MATCH (p:Creator)-[r:coauthored]->(q:Creator)
      RETURN p as source, q as target, r.count as count
    """
  )
  return list(res)

def calculate_squared_relations(tx, author_pid: str):
  res = tx.run(
    """
      MATCH (p:Author {dblp_id:$author_pid})-[r:coauthored_with]->(q:Author)-[s:coauthored_with]->(t:Author)
      where not t.dblp_id = p.dblp_id
      RETURN p.dblp_id AS source_pid, t.dblp_id AS target_pid, r.count AS count1, s.count AS count2
    """,
    author_pid=author_pid
    )
  
  squared_edges = defaultdict(lambda : [0,0])
  
  for record in res:
    source_pid = record["source_pid"]
    target_pid = record["target_pid"]
    count1 = record["count1"]
    count2 = record["count2"]
    
    squared_edges[source_pid + "_" + target_pid][0] += count1
    squared_edges[source_pid + "_" + target_pid][1] += count2
  return squared_edges


def calculate_squared_relations_creators(tx, author_pid: str):
  res = tx.run(
    """
      MATCH (p:Creator {pid:$author_pid})-[r:coauthored]->(q:Creator)-[s:coauthored]->(t:Creator)
      where not t.pid = p.pid
      RETURN p.pid AS source_pid, t.pid AS target_pid, r.count AS count1, s.count as count2
    """,
    author_pid=author_pid
  )

  squared_edges = defaultdict(lambda: [0, 0])

  for record in res:
    source_pid = record["source_pid"]
    target_pid = record["target_pid"]
    count1 = record["count1"]
    count2 = record["count2"]

    squared_edges[source_pid + "_" + target_pid][0] += count1
    squared_edges[source_pid + "_" + target_pid][1] += count2
  return squared_edges

def make_squared_relation(tx, source_pid: str, target_pid: str, count: int):
  return tx.run(
    """
      MATCH (p:Author {dblp_id: $source_pid})
      MATCH (q:Author {dblp_id: $target_pid})
      MERGE (p)-[:squared {count:$count}]->(q)
    """,
    source_pid=source_pid,
    target_pid=target_pid,
    count=count,
  )


def make_squared_relation_for_creator(tx, source_pid: str, target_pid: str, count: int):
  return tx.run(
    """
      MATCH (p:Creator {pid: $source_pid})
      MATCH (q:Creator {pid: $target_pid})
      MERGE (p)-[:squared_rel {count:$count}]->(q)
    """,
    source_pid=source_pid,
    target_pid=target_pid,
    count=count,
  )


def get_squared_relations(tx):
  res = tx.run(
    """
      MATCH (p:Author)-[r:squared]->(q:Author)
      RETURN p as source, q as target, r.count as count
    """
  )
  return list(res)


def get_squared_relations_creator(tx):
  res = tx.run(
    """
      MATCH (p:Creator)-[r:squared_rel]->(q:Creator)
      RETURN p as source, q as target, r.count as count
    """
  )
  return list(res)


def get_wrong_matched_dblp_authors(tx):
  res = tx.run(
    """
      MATCH (n:Author)
      WHERE NOT (n)-[:coauthored_with]-()
      RETURN n
      ORDER BY n.name
    """
  )
  return list(res)


def make_creator(tx, creator_name, aid, pid):
  return tx.run(
    """
      CREATE (n: Creator{name: $name, aid: $aid, pid: $pid})
    """,
    name=creator_name,
    aid=aid,
    pid=pid
  )

