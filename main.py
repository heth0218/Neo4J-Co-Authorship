from db import (
  GraphDBDriver,
  find_coauthor_relations,
  find_squared_relations,
  find_all_relations,
  get_coauthor_relations,
  get_squared_relations, get_creator_relations, get_squared_relations_creator
)
from utils import (
  create_viz_json,
  read_authors_file,
  seed_authors,
  seed_relations,
  seed_squared_relations,
  scrape_tabular_data,
  create_author_from_csv,
  add_valid_alex_id,
  search_authors_in_dblp, get_name_match_list,
  get_details_from_closet_data_json_save_to_neo, create_viz_json_creator
)
from constants import (
  URI, 
  USER, 
  PASSWORD
)
import asyncio

if __name__ == "__main__":

  '''
    Uncomment the following lines to scrape data
  '''
  # url="https://www.iscaconf.org/isca2023/committees/pc.php"
  # scrape_tabular_data(url=url)
  db = GraphDBDriver(URI, USER, PASSWORD)
  db.verify_connectivity()

  '''
    Uncomment this line to create new authors in neo4j db
  '''
  # create_author_from_csv(db.driver)

  '''
    Uncomment this line to add OpenAlex details for the authors in db
  '''
  # add_valid_alex_id(db.driver)

  '''
    Search Authors in DBLP
  '''
  search_authors_in_dblp(db.driver)

  # get_details_from_closet_data_json_save_to_neo(db.driver)
  '''
    Old code to seed authors
  '''
  # author_names, disambiguated_author_names = read_authors_file("./data/sigmod_2021_program_committee.csv")
  # seed_authors(db.driver, author_names, disambiguated_author_names)
  # seed_relations(db.driver)
  # seed_squared_relations(db.driver)
  # get_name_match_list(db.driver)
  config = {
    "parameters": {
      "attraction": 1,
      "repulsion": -1,
      "min_cluster_size": 1,
      "merge_small_clusters": True,
      "normalization": "linlog/modularity",
      "iterations": 10000
    }
  }
  # create_viz_json("./output/coauthor_viz_1.json", db.driver, get_coauthor_relations, config)
  # create_viz_json_creator("./output/coauthor_creator_viz.json", db.driver, get_creator_relations, config)

  # create_viz_json("./output/squared_viz.json", db.driver, get_squared_relations, config)
  # create_viz_json_creator("./output/squared_viz.json", db.driver, get_squared_relations_creator, config)

  # create_viz_json("./output/union_viz.json", db.driver, find_all_relations, config)