import requests
import xml.etree.ElementTree as ET
import json
import aiohttp
import asyncio
import csv
import logging
import time
import xmltodict
from requests.utils import quote
from tqdm import tqdm
from constants import DB
from db import (
    create_author,
    find_author,
    find_authors,
    create_relation,
    create_squared_relation,
    compute_squared_relations,
    make_author,
    get_author_by_name,
    update_author_alex_details_by_id,
    get_alex_unfound_authors,
    get_all_authors,
    update_author_dblp_info_by_id,
    get_authors_not_found_on_dblp,
    get_author_by_dblp_id,
    make_relation,
    calculate_squared_relations,
    make_squared_relation, get_wrong_matched_dblp_authors,
    make_creator, get_creators, update_creator_dblp_info_by_id, get_all_creators, get_creator_by_dblp_id,
    make_creator_relation, make_squared_relation_for_creator, calculate_squared_relations_creators,
    get_wrong_dblp_authors, get_all_authors_temp, get_max_aid, make_author_mutable, update_author_dblp_info_by_name,
    get_unconnected_people
)
import pandas as pd
from bs4 import BeautifulSoup

logging.basicConfig(
    filename='./output/output.log',
    filemode='w',
    encoding='utf-8',
    level=logging.INFO,
    format='%(asctime)s~[%(levelname)s]~%(message)s',
    datefmt='(%d %B %Y %H:%M:%S)'
)


def read_authors_file(src_path: str, delimiter: str = ","):
    print("[INFO]: Reading author file at {}".format(src_path))
    authors_names = []
    disambiguated_authors_names = set([])

    with open(src_path, mode="r") as f:
        lines = csv.reader(f, delimiter=delimiter)

        for line in lines:
            if len(line) != 0:
                authors_names.append(line[0].strip())

                if len(line) == 3:
                    disambiguated_authors_names.add(line[-1].strip())
    return authors_names, disambiguated_authors_names


def call_author_api(author_name: str):
    resp = requests.get("https://dblp.org/search/author/api?q={}$&format=json".format(quote(author_name)))
    resp.raise_for_status()
    data = resp.json()
    return data


architecture_journals = ('ISCA', 'IEEE MICRO', 'HPCA',
                         'Architectural support for languages and operating systems',
                         'ASPLOS',
                         'International Symposium on Computer Architecture',
                         'Symposium on Microprocessor Architectures')

information_journal = (
    'SIGIR',
    'CIKM'
)

natural_language_processing =(
    'ACL',
    'EMNLP'
)

human_computer_interaction= (
    'CHI'
)

programming_language = (
    'PLDI',
    'POPL',
    'OOPSLA',
    'ICFP'
)

def get_author_detail_full_iteration(hits: list, disambiguated_authors: set[str]):
    author_info = None
    if len(hits) > 1:
        found = False
        freq_dict = {}
        for author in hits:
            author_name = author.get("info").get("author")
            id = author.get('@id')
            url_1 = author.get("info").get("url")
            url_1 = url_1.split("/")
            pid = url_1[-2] + "/" + url_1[-1]
            # try:
            #     time.sleep(2)
            #     resp = requests.get(
            #         "https://dblp.org/search/publ/api?q=author:{}&format=json&h=1000".format(quote(author_name)))
            #     resp.raise_for_status()
            #
            #     data = resp.json()
            # except requests.exceptions.JSONDecodeError as json_err:
            #     print(f"JSON decoding error occurred: {json_err}")
            #     continue
            # except requests.exceptions.HTTPError as e:
            #     if e.response.status_code == 500:
            #         # Handle the 500 Internal Server Error gracefully
            #         print("Received a 500 Internal Server Error. Continuing with the code...")
            #     else:
            #         # Handle other HTTP errors as needed
            #         print(f"HTTP Error: {e.response.status_code}")
            #     continue
            # except requests.exceptions.RequestException as e:
            #     # Handle other types of network or request-related errors
            #     print(f"Request Exception: {e}")
            #     continue

            # h = data.get('result').get('hits').get('hit') if data.get('result') and data.get('result').get(
            #     'hits') and data.get('result').get('hits').get('hit') else []

            # for d in h:
            #     if d.get('info') and d.get('info').get('venue') in ('ISCA', 'IEEE MICRO', 'HPCA',
            #                                                         'Architectural support for languages and operating systems',
            #                                                         'ASPLOS',
            #                                                         'International Symposium on Computer Architecture',
            #                                                         'Symposium on Microprocessor Architectures'):
            #         author_info = author.get("info")
            #         if freq_dict.get(id):
            #             freq_dict[id] = {'author_info': author_info,
            #                              'frequency': freq_dict[id]['frequency'] + 1}
            #         else:
            #             freq_dict[id] = {'author_info': author_info,
            #                              'frequency': 1}
            #         found = True

            xml_url = 'https://dblp.org/pid/{}.xml'.format(quote(pid))

            try:
                time.sleep(2)
                response = requests.get(xml_url)

                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    # Parse the XML content into a dictionary
                    xml_dict = xmltodict.parse(response.content)
                    for x in xml_dict['dblpperson']['r']:
                        if isinstance(x, str):
                            continue
                        journal = ''
                        if x.get('article'):
                            journal = x['article']['booktitle'] if x['article'].get('booktitle') else x['article'][
                                'journal']
                        elif x.get('inproceedings'):
                            journal = x['inproceedings']['booktitle'] if x['inproceedings'].get('booktitle') else \
                                x['inproceedings']['journal']
                        elif x.get('proceedings'):
                            journal = x['proceedings']['booktitle'] if x['proceedings'].get('booktitle') else \
                                x['proceedings']['publisher']
                        elif x.get('incollection'):
                            journal = x['incollection']['booktitle'] if x['incollection'].get('booktitle') else \
                                x['incollection']['journal']

                        # if journal in information_journal:
                        if contains_substring(journal, programming_language):
                            author_info = author.get("info")
                            if freq_dict.get(id):
                                freq_dict[id] = {'author_info': author_info,
                                                 'frequency': freq_dict[id]['frequency'] + 1}
                            else:
                                freq_dict[id] = {'author_info': author_info,
                                                 'frequency': 1}
                            found = True

                else:
                    print(f'Failed to retrieve data. Status code: {response.status_code}')

            except requests.exceptions.RequestException as e:
                print(f'Error: {e}')
            except xmltodict.expat.ExpatError as e:
                print(f'Error parsing XML: {e}')
        if found:
            mx = 0
            for key in freq_dict:
                if freq_dict[key]['frequency'] >= mx:
                    mx = freq_dict[key]['frequency']
                    author_info = freq_dict[key]['author_info']
            print('final author-', author_info)

        if not found:
            for author in hits:
                if author.get("info").get("author") in disambiguated_authors:
                    author_info = author.get("info")
                    found = True
                    break

        if not found:
            author_info = hits[0].get("info")

    else:
        author_info = hits[0].get("info")

    author_details = {}
    author_details["name"] = author_info.get("author", None)
    author_details["url"] = author_info.get("url", None)

    if author_info.get("url", False):
        url = author_info["url"].split("/")
        author_details["pid"] = url[-2] + "/" + url[-1]

    author_details["affiliations"] = None

    if author_info.get("notes", False) and author_info["notes"].get("note", False):
        affiliations = []

        if type(author_info["notes"]["note"]) == dict and author_info["notes"]["note"].get("@type",
                                                                                           False) == "affiliation":
            affiliations.append(author_info["notes"]["note"]["text"])
        elif type(author_info["notes"]["note"]) == list:
            for i in author_info["notes"]["note"]:
                if i.get("@type", False) == "affiliation":
                    affiliations.append(i["text"])
        author_details["affiliations"] = affiliations

    author_details["aliases"] = None

    if author_info.get("aliases", False) and author_info["aliases"].get("alias", False):
        aliases = []
        if type(author_info["aliases"]["alias"]) == str:
            aliases.append(author_info["aliases"]["alias"])
        elif type(author_info["aliases"]["alias"]) == list:
            for i in author_info["aliases"]["alias"]:
                aliases.append(i)
        author_details["aliases"] = aliases
    return author_details

def get_author_details(hits: list, disambiguated_authors: set[str]):
    author_info = None

    if len(hits) > 1:
        # disambiguate author
        found = False
        for author in hits:
            author_name = author.get("info").get("author")

            # resp = requests.get(
            #     "https://dblp.org/search/publ/api?q=author:{}&format=json&h=1000".format(quote(author_name)))
            # if resp.status_code == 500:
            #     continue
            # print(author_name)
            # data = resp.json()
            try:
                time.sleep(2)
                resp = requests.get(
                    "https://dblp.org/search/publ/api?q=author:{}&format=json&h=1000".format(quote(author_name)))
                resp.raise_for_status()

                data = resp.json()

                # Rest of your code for JSON processing
            except requests.exceptions.JSONDecodeError as json_err:
                print(f"JSON decoding error occurred: {json_err}")
                continue
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 500:
                    # Handle the 500 Internal Server Error gracefully
                    print("Received a 500 Internal Server Error. Continuing with the code...")
                else:
                    # Handle other HTTP errors as needed
                    print(f"HTTP Error: {e.response.status_code}")
                continue
            except requests.exceptions.RequestException as e:
                # Handle other types of network or request-related errors
                print(f"Request Exception: {e}")
                continue

            h = data.get('result').get('hits').get('hit') if data.get('result') and data.get('result').get(
                'hits') and data.get('result').get('hits').get('hit') else []
            for d in h:
                if d.get('info') and d.get('info').get('venue') in ('ISCA', 'IEEE MICRO', 'HPCA',
                                                                    'Architectural support for languages and operating systems',
                                                                    'ASPLOS',
                                                                    'International Symposium on Computer Architecture',
                                                                    'Symposium on Microprocessor Architectures'):
                    # if d.get('info') and d.get('info').get('venue') in ('SIGMOD Conference', 'Proc. VLDB Endow.', 'ICDE'):
                    author_info = author.get("info")
                    print('a', author_info)
                    found = True
                    break
            if found:
                break

        if not found:
            for author in hits:
                if author.get("info").get("author") in disambiguated_authors:
                    author_info = author.get("info")
                    found = True
                    break

        if not found:
            author_info = hits[0].get("info")
    else:
        author_info = hits[0].get("info")

    author_details = {}
    author_details["name"] = author_info.get("author", None)
    author_details["url"] = author_info.get("url", None)

    if author_info.get("url", False):
        url = author_info["url"].split("/")
        author_details["pid"] = url[-2] + "/" + url[-1]

    author_details["affiliations"] = None

    if author_info.get("notes", False) and author_info["notes"].get("note", False):
        affiliations = []

        if type(author_info["notes"]["note"]) == dict and author_info["notes"]["note"].get("@type",
                                                                                           False) == "affiliation":
            affiliations.append(author_info["notes"]["note"]["text"])
        elif type(author_info["notes"]["note"]) == list:
            for i in author_info["notes"]["note"]:
                if i.get("@type", False) == "affiliation":
                    affiliations.append(i["text"])
        author_details["affiliations"] = affiliations

    author_details["aliases"] = None

    if author_info.get("aliases", False) and author_info["aliases"].get("alias", False):
        aliases = []
        if type(author_info["aliases"]["alias"]) == str:
            aliases.append(author_info["aliases"]["alias"])
        elif type(author_info["aliases"]["alias"]) == list:
            for i in author_info["aliases"]["alias"]:
                aliases.append(i)
        author_details["aliases"] = aliases
    return author_details


def seed_authors(db, authors: list[str], disambiguated_authors: set[str]):
    print("[INFO]: Seeding authors into database...")
    seen_authors = set()
    missed_authors = []

    with db.session(database=DB) as session:
        for author in tqdm(authors):
            try:
                author_data = call_author_api(author)
            except Exception as err:
                print(err)

            hits = author_data.get("result", 0).get("hits", 0)
            no_of_hits = int(hits.get("@total", 0))

            if no_of_hits > 0:
                author_details = get_author_details(hits["hit"], disambiguated_authors)

                if author_details == None or author_details["pid"] in seen_authors:
                    continue
                seen_authors.add(author_details["pid"])
                session.execute_write(create_author, author_details)
            else:
                missed_authors.append(author)
        if len(missed_authors) > 0:
            logging.info("Missed authors: {}".format(missed_authors))


def call_dblp_coauthor_api(author_pid: str):
    coauthor_xml = requests.get("https://dblp.org/pid/" + author_pid + ".xml?view=coauthor")
    return coauthor_xml.content


def parse_coauthors_xml(session, coauthors_xml_tree: str, source_pid: str):
    root = ET.fromstring(coauthors_xml_tree)

    for coauthor in root.findall("author"):
        target_pid = coauthor.get("pid")
        if session.execute_read(get_author_by_dblp_id, target_pid) != None:
            pub_count = int(coauthor.get("count"))
            session.execute_write(make_relation, source_pid, target_pid, pub_count)


def seed_relations(db):
    print("[INFO]: Seeding relations into database...")
    with db.session(database=DB) as session:
        author_pids = session.execute_read(get_all_authors)
        for source_pid in tqdm(author_pids):
            try:
                coauthors_xml_tree = call_dblp_coauthor_api(source_pid)
                parse_coauthors_xml(session, coauthors_xml_tree, source_pid)
            except ET.ParseError as errp:
                print(errp)
            except Exception as err:
                print(err)


def seed_squared_relations(db):
    print("[INFO]: Seeding squared relations into database...")
    with db.session(database=DB) as session:
        author_pids = session.execute_read(get_all_authors)

        for source_pid in tqdm(author_pids):
            squared_edges = session.execute_read(calculate_squared_relations, source_pid)

            for i, j in squared_edges.items():
                src_pid, tr_pid = i.split("_")
                session.execute_write(make_squared_relation, src_pid, tr_pid, j[0] + j[1])

def get_name(dblp_id):
    resp = requests.get(dblp_id+'.xml')
    author_data = resp.text

    # Parse the XML data
    root = ET.fromstring(author_data)
    #
    # # Find the name attribute in dblpperson element
    name = root.get("name")
    print("Name:", name)
    return name



def create_viz_json(dst_path: str, db, relation_func, config: dict):
    network = {"items": [], "links": []}
    seen_relations = set([])
    seen_authors = set([])

    chinese_authors = []

    with db.session(database=DB) as session:
        res = session.execute_read(relation_func)
        print(len(res))

    for relation in res:
        relation_data = relation.data()
        source = relation_data["source"]
        source_affiliation = source.get("affiliations", None)
        target = relation_data["target"]
        target_affiliation = target.get("affiliations", None)
        count = int(relation_data["count"])

        if source["dblp_id"] not in seen_authors:
            author_nam = source.get("name", "")
            name_parts = author_nam.split()
            first_name = " ".join(name_parts[:-1])
            last_name = name_parts[-1]
            if last_name in [
                "Wang", "Li", "Zhang", "Liu", "Chen", "Yang", "Huang", "Zhao", "Wu", "Zhou",
                "Xu", "Sun", "Ma", "Zhu", "Hu", "Guo", "Lin", "He", "Gao", "Luo",
                "Zheng", "Liang", "Xie", "Tang", "Zhuang", "Shao", "Kong", "Cao", "Deng", "Jin",
                "Feng", "Yu", "Lu", "Jiang", "Song", "Yuan", "Han", "Yan", "Feng", "Chen",
                "Qian", "Xia", "Wu", "Ou", "Zhang", "Wei", "Xiong", "Sheng", "Pan", "Kang",
                "Zeng", "Cheng", "Guan", "Tang", "Xiong", "Yao", "He", "Ye", "Shen", "Jin",
                "Shuai", "Cui", "Geng", "Long", "Lu", "Yun", "Geng", "Jia", "Qiao", "Zhuo",
                "Shi", "Yuan", "Qi", "Jin", "Su", "Mo", "Du", "Huang", "Lai", "Gu",
                "Shang", "Qu", "Yan", "Du", "Zhuo", "Lu", "Jiang", "Peng", "Yao", "Tan",
                "Guo", "Dong", "Yao", "Tang", "Zhong", "Yao", "Gu", "Xue", "Zhao", "Yang",
                "Chen", "Lin", "Huang", "Chang", "Lee", "Wang", "Wu", "Liu", "Tsai", "Yang",
                "Hsu", "Cheng", "Hsieh", "Kuo", "Liang", "Chung", "Hung", "Chiu", "Lai", "Ruan", "Ng", "Hua",
                "Kim", "Wen"
            ]:
                chinese_authors.append({
                    'name': author_nam,
                    'dblp name': get_name("https://dblp.org/pid/{}".format(source["dblp_id"])),
                    'dblp url':"https://dblp.org/pid/{}".format(source["dblp_id"])
                })
            network["items"].append({
                "id": source["dblp_id"],
                "label": source.get("name", ""),
                "url": "https://dblp.org/pid/{}".format(source["dblp_id"]),
                "description": "<h2><b>{}</b></h2><h3>dblp_id: {}</h3><h3>DBLP URL: <a href='{}'>{}</a></h3><h3>Affiliation: {}</h3>" \
                    .format(
                    source["name"],
                    source["dblp_id"],
                    "https://dblp.org/pid/{}".format(source["dblp_id"]),
                    "https://dblp.org/pid/{}".format(source["dblp_id"]),
                    "" if source_affiliation is None else ", ".join(source_affiliation)
                ),
            })
            seen_authors.add(source["dblp_id"])

        if target["dblp_id"] not in seen_authors:
            author_nam = target.get("name", "")
            name_parts = author_nam.split()
            first_name = " ".join(name_parts[:-1])
            last_name = name_parts[-1]
            if last_name in [
                "Wang", "Li", "Zhang", "Liu", "Chen", "Yang", "Huang", "Zhao", "Wu", "Zhou",
                "Xu", "Sun", "Ma", "Zhu", "Hu", "Guo", "Lin", "He", "Gao", "Luo",
                "Zheng", "Liang", "Xie", "Tang", "Zhuang", "Shao", "Kong", "Cao", "Deng", "Jin",
                "Feng", "Yu", "Lu", "Jiang", "Song", "Yuan", "Han", "Yan", "Feng", "Chen",
                "Qian", "Xia", "Wu", "Ou", "Zhang", "Wei", "Xiong", "Sheng", "Pan", "Kang",
                "Zeng", "Cheng", "Guan", "Tang", "Xiong", "Yao", "He", "Ye", "Shen", "Jin",
                "Shuai", "Cui", "Geng", "Long", "Lu", "Yun", "Geng", "Jia", "Qiao", "Zhuo",
                "Shi", "Yuan", "Qi", "Jin", "Su", "Mo", "Du", "Huang", "Lai", "Gu",
                "Shang", "Qu", "Yan", "Du", "Zhuo", "Lu", "Jiang", "Peng", "Yao", "Tan",
                "Guo", "Dong", "Yao", "Tang", "Zhong", "Yao", "Gu", "Xue", "Zhao", "Yang",
                "Chen", "Lin", "Huang", "Chang", "Lee", "Wang", "Wu", "Liu", "Tsai", "Yang",
                "Hsu", "Cheng", "Hsieh", "Kuo", "Liang", "Chung", "Hung", "Chiu", "Lai", "Ruan", "Ng", "Hua",
                "Kim", "Wen"
            ]:
                chinese_authors.append({
                    'name': author_nam,
                    'dblp name': get_name("https://dblp.org/pid/{}".format(target["dblp_id"])),
                    'dblp url': "https://dblp.org/pid/{}".format(target["dblp_id"])
                })
            network["items"].append({
                "id": target["dblp_id"],
                "label": target.get("name", ""),
                "url": "https://dblp.org/pid/{}".format(target["dblp_id"]),
                "description": "<h2><b>{}</b></h2><h3>dblp_id: {}</h3><h3>DBLP URL: <a href='{}'>{}</a></h3><h3>Affiliation: {}</h3>" \
                    .format(
                    target["name"],
                    target["dblp_id"],
                    "https://dblp.org/pid/{}".format(target["dblp_id"]),
                    "https://dblp.org/pid/{}".format(target["dblp_id"]),
                    "" if target_affiliation is None else ", ".join(target_affiliation)
                ),
            })
            seen_authors.add(target["dblp_id"])

        if "{}${}".format(source["dblp_id"], target["dblp_id"]) not in seen_relations and \
                "{}${}".format(target["dblp_id"], source["dblp_id"]) not in seen_relations:
            network["links"].append({
                "source_id": source["dblp_id"],
                "target_id": target["dblp_id"],
                "strength": count
            })

    resp = {"network": network}
    print(len(seen_authors))
    if config is not None:
        resp["config"] = config

    with open(dst_path, "w", encoding="utf-8") as f:
        json.dump(resp, f, ensure_ascii=False, indent=4)
    print(chinese_authors)

def create_viz_json_creator(dst_path: str, db, relation_func, config: dict):
    network = {"items": [], "links": []}
    seen_relations = set([])
    seen_authors = set([])

    with db.session(database=DB) as session:
        res = session.execute_read(relation_func)

    for relation in res:
        relation_data = relation.data()
        source = relation_data["source"]
        source_affiliation = source.get("affiliations", None)
        target = relation_data["target"]
        target_affiliation = target.get("affiliations", None)
        count = int(relation_data["count"])

        if source["pid"] not in seen_authors:
            network["items"].append({
                "id": source["pid"],
                "label": source.get("name", ""),
                "url": "https://dblp.org/pid/{}".format(source["pid"]),
                "description": "<h2><b>{}</b></h2><h3>dblp_id: {}</h3><h3>DBLP URL: <a href='{}'>{}</a></h3><h3>Affiliation: {}</h3>" \
                    .format(
                    source["name"],
                    source["pid"],
                    "https://dblp.org/pid/{}".format(source["pid"]),
                    "https://dblp.org/pid/{}".format(source["pid"]),
                    "" if source_affiliation is None else ", ".join(source_affiliation)
                ),
            })
            seen_authors.add(source["pid"])

        if target["pid"] not in seen_authors:
            network["items"].append({
                "id": target["pid"],
                "label": target.get("name", ""),
                "url": "https://dblp.org/pid/{}".format(target["pid"]),
                "description": "<h2><b>{}</b></h2><h3>dblp_id: {}</h3><h3>DBLP URL: <a href='{}'>{}</a></h3><h3>Affiliation: {}</h3>" \
                    .format(
                    target["name"],
                    target["pid"],
                    "https://dblp.org/pid/{}".format(target["pid"]),
                    "https://dblp.org/pid/{}".format(target["pid"]),
                    "" if target_affiliation is None else ", ".join(target_affiliation)
                ),
            })
            seen_authors.add(target["pid"])

        if "{}${}".format(source["pid"], target["pid"]) not in seen_relations and \
                "{}${}".format(target["pid"], source["pid"]) not in seen_relations:
            network["links"].append({
                "source_id": source["pid"],
                "target_id": target["pid"],
                "strength": count
            })

    resp = {"network": network}

    if config is not None:
        resp["config"] = config

    with open(dst_path, "w", encoding="utf-8") as f:
        json.dump(resp, f, ensure_ascii=False, indent=4)


def get_names_and_affiliations(committee_html: list, names: list = [], affiliations=[]):
    for i in committee_html.find_all("tr")[1:]:
        txt = i.text
        names.append(txt.splitlines()[1])
        affiliations.append(txt.splitlines()[2])
    return names, affiliations


def scrape_tabular_data(url: str):
    if url:
        try:
            r = requests.get(url)
            soup = BeautifulSoup(r.text, "lxml")
            if "2019" not in url:
                table = soup.find_all("table", class_="table table-striped table-hover table-condensed")
                external_review_committee_members_2 = table[-1]
                external_review_committee_members_1 = table[-2]

                program_committee_members_2 = table[-3]
                program_committee_members_1 = table[-4]

                names, affiliations = get_names_and_affiliations(program_committee_members_1, [], [])
                names, affiliations = get_names_and_affiliations(program_committee_members_2, names=names,
                                                                 affiliations=affiliations)
                df = pd.DataFrame({'names': names, 'affiliations': affiliations})
                df.to_csv('./data/isca_{}_program_committee.csv', header=False, index=False)

                names, affiliations = get_names_and_affiliations(external_review_committee_members_1, [], [])
                names, affiliations = get_names_and_affiliations(external_review_committee_members_2, names=names,
                                                                 affiliations=affiliations)
                df = pd.DataFrame({'names': names, 'affiliations': affiliations})
                df.to_csv('./data/isca_{}_external_review_committee.csv', header=False, index=False)
            else:
                # Exclusive scraping for 2019
                div = soup.find_all("div", class_="table-wrapper")
                table = []
                for d in div:
                    table.append(d.find_all("table")[0])
                # print(table)
                external_review_committee_members = table[-1]

                program_committee_members = table[-2]

                names, affiliations = get_names_and_affiliations(program_committee_members, [], [])
                for i in range(0, len(names)):
                    names[i] = names[i].strip()

                for i in range(0, len(affiliations)):
                    affiliations[i] = affiliations[i].strip()

                df = pd.DataFrame({'names': names + affiliations})
                df.to_csv('./data/isca_{}_program_committee.csv', header=False, index=False)

                names, affiliations = get_names_and_affiliations(external_review_committee_members, [], [])
                for i in range(0, len(names)):
                    names[i] = names[i].strip()

                for i in range(0, len(affiliations)):
                    affiliations[i] = affiliations[i].strip()

                df = pd.DataFrame({'names': names + affiliations})
                df.to_csv('./data/isca_{}_external_review_committee.csv', header=False, index=False)

        except Exception as e:
            print(e)


def read_data_file():
    df = pd.read_csv('Supercomputing_PC.csv', names=['name', 'affiliation'], quotechar='"',
                     encoding="utf-8")
    print(df)

    def some_function(df):
        dic = {}

        Universities = list(df.affiliation.unique())
        result_string = ', '.join(str(item) for item in Universities)
        result_string = "[" + result_string + "]"
        dic = {
            'name': df.iloc[0]['name'],
            'affiliation': result_string
        }
        new_df = pd.DataFrame(dic, index=[0])
        return new_df

    Newdf = df.groupby("name").apply(some_function)

    Newdf.to_csv('Supercomputing_PC_Final.csv', header=False, index=False)


def create_author_from_csv(db):
    print("[INFO]: Seeding authors into database...")
    df = pd.read_csv('PL_PC_Final.csv', names=['name', 'affiliation'], quotechar='"',
                     encoding="utf-8")
    with db.session(database=DB) as session:
        for index, row in df.iterrows():
            affiliation_list = row['affiliation'].strip('][').split(', ')
            author_details = {
                'aid': index,
                'name': row['name'],
                'affiliations': affiliation_list
            }
            session.execute_write(make_author, author_details)


def add_valid_alex_id(db):
    print("[INFO]: Adding Alex info to Neo4j db...")

    with db.session(database=DB) as session:
        df = pd.read_csv('data/program_committee/isca_program_committee.csv', names=['name', 'affiliation'],
                         quotechar='"', encoding="utf-8")
        for index, row in df.iterrows():
            author_name = row['name']
            res = session.execute_read(get_author_by_name, author_name)
            print(author_name)
            response = res.data()['a']
            author_name = response['name']
            affiliations = response['affiliations']
            aid = response['aid']
            a_institute = response['alex_institute'] if response.get('alex_institute') else None

            university_ids = set()
            for institute in affiliations:
                university = requests.get(
                    "https://api.openalex.org/institutions?filter=display_name.search:{}".format(quote(institute)))
                if university is not None:
                    university = university.json()
                    if university['meta']['count'] >= 1:
                        university_ids.add(university['results'][0]['id'])

            resp = requests.get(
                "https://api.openalex.org/authors?filter=display_name.search:{}".format(quote(author_name)))
            author_info = resp.json()

            author_open_alex_id = None
            author_open_alex_name = None
            author_open_alex_institute_name = None
            author_open_alex_institute_id = None

            if author_info is not None:
                if author_info['meta']['count'] >= 1:
                    results = author_info['results']
                    for result in results:
                        if result['last_known_institution'] is not None:
                            last_institute_id = result['last_known_institution']['id']
                            if last_institute_id in university_ids:
                                author_open_alex_id = result['id']
                                author_open_alex_name = result['display_name']
                                author_open_alex_institute_name = result['last_known_institution']['display_name']
                                author_open_alex_institute_id = last_institute_id
                                break
                    if author_open_alex_id is None:
                        for result in results:
                            works = requests.get(
                                "https://api.openalex.org/works?filter=authorships.author.id:{}".format(
                                    quote(result['id'])))
                            works = works.json()
                            if works['meta']['count'] >= 1:
                                author_works = works['results']
                                for author_work in author_works:
                                    if author_work['doi'] and 'isca' in author_work['doi']:
                                        author_open_alex_id = result['id']
                                        author_open_alex_name = result['display_name']
                                        author_open_alex_institute_name = result['last_known_institution'][
                                            'display_name'] if result['last_known_institution'] else None
                                        author_open_alex_institute_id = result['last_known_institution']['id'] if \
                                            result['last_known_institution'] else None
                                        break
                                if author_open_alex_id is None:
                                    for author_work in author_works:
                                        if author_work['primary_location'] and author_work['primary_location'][
                                            'source'] and author_work['primary_location']['source']['display_name'] and \
                                                author_work['primary_location']['source'][
                                                    'display_name'] == 'IEEE Micro':
                                            author_open_alex_id = result['id']
                                            author_open_alex_name = result['display_name']
                                            author_open_alex_institute_name = result['last_known_institution'][
                                                'display_name'] if result['last_known_institution'] else None
                                            author_open_alex_institute_id = result['last_known_institution']['id'] if \
                                                result['last_known_institution'] else None
                                            break
                                        elif author_work['locations']:
                                            for aw in author_work['locations']:
                                                if aw['source'] and aw['source']['display_name'] and aw['source'][
                                                    'display_name'] in (
                                                        'IEEE Micro',
                                                        'International Symposium on Computer Architecture',
                                                        'High-Performance Computer Architecture'):
                                                    author_open_alex_id = result['id']
                                                    author_open_alex_name = result['display_name']
                                                    author_open_alex_institute_name = result['last_known_institution'][
                                                        'display_name'] if result['last_known_institution'] else None
                                                    author_open_alex_institute_id = result['last_known_institution'][
                                                        'id'] if result['last_known_institution'] else None
                                                    break
                    if author_open_alex_id is None:
                        author_open_alex_id = results[0]['id']
                        author_open_alex_name = results[0]['display_name']
                        author_open_alex_institute_name = results[0]['last_known_institution']['display_name'] if \
                            results[0]['last_known_institution'] else None
                        author_open_alex_institute_id = results[0]['last_known_institution']['id'] if results[0][
                            'last_known_institution'] else None
            if a_institute is not None and a_institute != author_open_alex_institute_name:
                print(author_open_alex_id, author_open_alex_name, author_open_alex_institute_name)
            session.execute_write(update_author_alex_details_by_id,
                                  aid=aid,
                                  alex_id=author_open_alex_id,
                                  alex_name=author_open_alex_name,
                                  alex_institute=author_open_alex_institute_name,
                                  alex_institute_id=author_open_alex_institute_id)
    print("[INFO]: Info successfully imported to db")



def get_disambiguated_list_details(last_name, first_name):
    url = f"https://dblp.uni-trier.de/pers/?prefix={last_name}+{first_name}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    element_with_id = soup.find(id='browse-person-output')

    final_arr_dict = []

    link2 = ''
    if element_with_id:
        ul_element = element_with_id.find('ul')

        if ul_element:
            for li in ul_element.find_all('li'):
                a_element = li.find('a')
                if a_element:
                    href_link = a_element.get('href')
                    link2 = href_link
                    break
        else:
            print("No <ul> element found inside the element with id 'example_id'.")
    else:
        print("No element with id 'example_id' found on the page.")

    def extract_id_from_url(url):
        url_parts = url.split('/')
        pid_index = url_parts.index('pid') if 'pid' in url_parts else -1
        if pid_index != -1 and pid_index + 1 < len(url_parts):
            return '/'.join(url_parts[pid_index + 1:]).split('.')[0]
        else:
            return None

    if link2:
        response = requests.get(link2)
        soup = BeautifulSoup(response.text, 'html.parser')
        element_with_id = soup.find(class_='expand-head')

        if element_with_id:
            for li in element_with_id.find_all('li'):
                a_element = li.find('a')
                s = ''
                for a_tag in a_element:
                    s += a_tag.text
                print(s)
                if a_element:
                    href_link = a_element.get('href')
                    print(href_link)

                    id_from_url = extract_id_from_url(href_link)
                    print(id_from_url)
                    dict = {'name': s, 'url': href_link, 'pid': id_from_url}
                    final_arr_dict.append(dict)
        element_with_id = soup.find(class_='expand-body')

        if element_with_id:
            for li in element_with_id.find_all('li'):
                a_element = li.find('a')
                s = ''
                for a_tag in a_element:
                    s += a_tag.text
                print(s)
                if a_element:
                    href_link = a_element.get('href')
                    print(href_link)

                    id_from_url = extract_id_from_url(href_link)
                    print(id_from_url)
                    dict = {'name': s, 'url': href_link, 'pid': id_from_url}
                    final_arr_dict.append(dict)
        element_with_id = soup.find(class_='hide-body hidden')
        if element_with_id:
            for li in element_with_id.find_all('li'):
                a_element = li.find('a')
                s = ''
                for a_tag in a_element:
                    s += a_tag.text
                print(s)
                if a_element:
                    href_link = a_element.get('href')
                    print(href_link)

                    id_from_url = extract_id_from_url(href_link)
                    print(id_from_url)
                    dict = {'name': s, 'url': href_link, 'pid': id_from_url}
                    final_arr_dict.append(dict)
    return final_arr_dict


def contains_substring(input_string, network_set):
    for network_substring in network_set:
        if network_substring in input_string:
            return True
    return False


def get_final_list_post_search(disambiguated_list):
    final_list=[]
    for index, author in enumerate(disambiguated_list):
        pid = author['pid']
        if pid:
            xml_url = 'https://dblp.org/pid/{}.xml'.format(quote(pid))
            found = False
            try:
                time.sleep(2)
                response = requests.get(xml_url)
                if response.status_code == 200:
                    xml_dict = xmltodict.parse(response.content)
                    for x in xml_dict['dblpperson']['r']:
                        if isinstance(x, str):
                            continue
                        journal = ''
                        if x.get('article'):
                            journal = x['article']['booktitle'] if x['article'].get('booktitle') else x['article'][
                                'journal']
                        elif x.get('inproceedings'):
                            journal = x['inproceedings']['booktitle'] if x['inproceedings'].get('booktitle') else \
                                x['inproceedings']['journal']
                        elif x.get('proceedings'):
                            journal = x['proceedings']['booktitle'] if x['proceedings'].get('booktitle') else \
                                x['proceedings']['publisher']
                        elif x.get('incollection'):
                            journal = x['incollection']['booktitle'] if x['incollection'].get('booktitle') else \
                                x['incollection']['journal']

                        if contains_substring(journal, programming_language):
                            final_list.append(author)
                            break
                else:
                    print(f'Failed to retrieve data. Status code: {response.status_code}')

            except requests.exceptions.RequestException as e:
                print(f'Error: {e}')
            except xmltodict.expat.ExpatError as e:
                print(f'Error parsing XML: {e}')

    return final_list


def dump_in_json_disambiguated_authors_for_manual_review(disambiguated_list, author_name):
    json_file_path = 'your_file.json'

    # Try to read existing content from the JSON file
    try:
        with open(json_file_path, 'r') as file:
            data = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {}

    # Add new key-value pairs to the data
    new_key = author_name
    new_value = disambiguated_list
    data[new_key] = new_value

    # Write the modified content back to the JSON file
    with open(json_file_path, 'w') as file:
        json.dump(data, file, indent=2)


def search_authors_in_dblp(db):
    with db.session(database=DB) as session:
        res = session.execute_read(get_wrong_dblp_authors)
        change_author_details = []

        for author_details in res:
            author = author_details.data()['n']
            author_nam = author['name']
            name_parts = author_nam.split()
            first_name = " ".join(name_parts[:-1])
            last_name = name_parts[-1]
            # print(f"https://dblp.org/search/author/api?q={quote(first_name)}-{quote(last_name)}$&format=json&h=1000")
            time.sleep(2)
            if last_name not in [
                "Wang", "Li", "Zhang", "Liu", "Chen", "Yang", "Huang", "Zhao", "Wu", "Zhou",
                "Xu", "Sun", "Ma", "Zhu", "Hu", "Guo", "Lin", "He", "Gao", "Luo",
                "Zheng", "Liang", "Xie", "Tang", "Zhuang", "Shao", "Kong", "Cao", "Deng", "Jin",
                "Feng", "Yu", "Lu", "Jiang", "Song", "Yuan", "Han", "Yan", "Feng", "Chen",
                "Qian", "Xia", "Wu", "Ou", "Zhang", "Wei", "Xiong", "Sheng", "Pan", "Kang",
                "Zeng", "Cheng", "Guan", "Tang", "Xiong", "Yao", "He", "Ye", "Shen", "Jin",
                "Shuai", "Cui", "Geng", "Long", "Lu", "Yun", "Geng", "Jia", "Qiao", "Zhuo",
                "Shi", "Yuan", "Qi", "Jin", "Su", "Mo", "Du", "Huang", "Lai", "Gu",
                "Shang", "Qu", "Yan", "Du", "Zhuo", "Lu", "Jiang", "Peng", "Yao", "Tan",
                "Guo", "Dong", "Yao", "Tang", "Zhong", "Yao", "Gu", "Xue", "Zhao", "Yang",
                "Chen", "Lin", "Huang", "Chang", "Lee", "Wang", "Wu", "Liu", "Tsai", "Yang",
                "Hsu", "Cheng", "Hsieh", "Kuo", "Liang", "Chung", "Hung", "Chiu", "Lai", "Ruan", "Ng", "Hua",
                "Kim", "Wen"
            ]:
                try:

                    resp = requests.get(
                        "https://dblp.org/search/author/api?q={}&format=json&h=1000".format(quote(author['name'])))
                    author_data = resp.json()
                except Exception as err:
                    print(err)

                hits = author_data.get("result", 0).get("hits", 0)
                no_of_hits = int(hits.get("@total", 0))

            else:
                print('lalal')
                try:
                    resp = requests.get(
                        f"https://dblp.org/search/author/api?q={quote(first_name)}-{quote(last_name)}$&format=json&h=1000")
                    author_data = resp.json()
                except Exception as err:
                    print(err)

                hits = author_data.get("result", 0).get("hits", 0)
                no_of_hits = int(hits.get("@total", 0))

            print(no_of_hits)
            if no_of_hits > 300:
                disambiguated_list = get_disambiguated_list_details(last_name, first_name)
                print(disambiguated_list)
                if disambiguated_list:
                    final_disambiguated_list = get_final_list_post_search(disambiguated_list)
                    if len(final_disambiguated_list)>0:
                        dump_in_json_disambiguated_authors_for_manual_review(final_disambiguated_list, author_nam)
                        continue
            if no_of_hits > 0:
                print(author['name'])
                author_details = get_author_detail_full_iteration(hits["hit"], disambiguated_authors=[author['name']])
                res = session.execute_read(get_author_by_name, a_name=author_nam)
                res = res.data()['a']
                if res.get('dblp_id') and res['dblp_id'] != author_details['pid']:
                    print("Details changed")

                session.execute_write(update_author_dblp_info_by_id, aid=author['aid'], dblp_id=author_details['pid'] if
                author_details else None)

#
# def search_authors_in_dblp(db):
#     with db.session(database=DB) as session:
#         # res = session.execute_read(get_authors_not_found_on_dblp)
#         # for closet data
#         res = session.execute_read(get_wrong_dblp_authors)
#         change_author_details = []
#
#         for author_details in res:
#             author = author_details.data()['n']
#             author_nam = author['name']
#             name_parts = author_nam.split()
#             first_name = " ".join(name_parts[:-1])
#             last_name = name_parts[-1]
#             # print(f"https://dblp.org/search/author/api?q={quote(first_name)}-{quote(last_name)}$&format=json&h=1000")
#             time.sleep(2)
#             if last_name not in [
#                 "Wang", "Li", "Zhang", "Liu", "Chen", "Yang", "Huang", "Zhao", "Wu", "Zhou",
#                 "Xu", "Sun", "Ma", "Zhu", "Hu", "Guo", "Lin", "He", "Gao", "Luo",
#                 "Zheng", "Liang", "Xie", "Tang", "Zhuang", "Shao", "Kong", "Cao", "Deng", "Jin",
#                 "Feng", "Yu", "Lu", "Jiang", "Song", "Yuan", "Han", "Yan", "Feng", "Chen",
#                 "Qian", "Xia", "Wu", "Ou", "Zhang", "Wei", "Xiong", "Sheng", "Pan", "Kang",
#                 "Zeng", "Cheng", "Guan", "Tang", "Xiong", "Yao", "He", "Ye", "Shen", "Jin",
#                 "Shuai", "Cui", "Geng", "Long", "Lu", "Yun", "Geng", "Jia", "Qiao", "Zhuo",
#                 "Shi", "Yuan", "Qi", "Jin", "Su", "Mo", "Du", "Huang", "Lai", "Gu",
#                 "Shang", "Qu", "Yan", "Du", "Zhuo", "Lu", "Jiang", "Peng", "Yao", "Tan",
#                 "Guo", "Dong", "Yao", "Tang", "Zhong", "Yao", "Gu", "Xue", "Zhao", "Yang",
#                 "Chen", "Lin", "Huang", "Chang", "Lee", "Wang", "Wu", "Liu", "Tsai", "Yang",
#                 "Hsu", "Cheng", "Hsieh", "Kuo", "Liang", "Chung", "Hung", "Chiu", "Lai", "Ruan", "Ng", "Hua",
#                 "Kim", "Wen"
#             ]:
#                 try:
#
#                     resp = requests.get(
#                         "https://dblp.org/search/author/api?q={}&format=json&h=1000".format(quote(author['name'])))
#                     author_data = resp.json()
#                 except Exception as err:
#                     print(err)
#
#                 hits = author_data.get("result", 0).get("hits", 0)
#                 no_of_hits = int(hits.get("@total", 0))
#
#             else:
#                 print('lalal')
#                 try:
#                     resp = requests.get(
#                         f"https://dblp.org/search/author/api?q={quote(first_name)}-{quote(last_name)}$&format=json&h=1000")
#                     author_data = resp.json()
#                 except Exception as err:
#                     print(err)
#
#                 hits = author_data.get("result", 0).get("hits", 0)
#                 no_of_hits = int(hits.get("@total", 0))
#             # if no_of_hits>35:
#             # try:
#             #     resp = requests.get(
#             #         "https://dblp.org/search/author/api?q={}$&format=json&h=1000".format(quote(author['name'])))
#             #     author_data = resp.json()
#             # except Exception as err:
#             #     print(err)
#             # hits = author_data.get("result", 0).get("hits", 0)
#             # no_of_hits = int(hits.get("@total", 0))
#             print(no_of_hits)
#             if no_of_hits > 0:
#                 print(author['name'])
#                 author_details = get_author_detail_full_iteration(hits["hit"], disambiguated_authors=[author['name']])
#                 # print(author_details)
#                 res = session.execute_read(get_author_by_name, a_name=author_nam)
#                 res = res.data()['a']
#                 # print(res)
#                 if res.get('dblp_id') and res['dblp_id'] != author_details['pid']:
#                     # print(
#                     #     f"Author details changes for {author_nam} and old dblp_id being {res['dblp_id']} and new one being {author_details['pid']}")
#                     # change_author_details.append({
#                     #     'author_name': author_nam,
#                     #     'old_dblp_url': f"https://dblp.org/pid/{res['dblp_id']}",
#                     #     'new_dblp_id': f"https://dblp.org/pid/{author_details['pid']}"
#                     # })
#                     # print(change_author_details)
#                     print("Details changed")
#
#                 session.execute_write(update_author_dblp_info_by_id, aid=author['aid'], dblp_id=author_details['pid'] if
#                 author_details else None)
#         # csv_file = "sample.csv"
#         #
#         # # Define the field names based on the dictionary keys
#         # field_names = change_author_details[0].keys()
#         #
#         # # Write the dictionary to the CSV file
#         # with open(csv_file, mode="w", newline="") as file:
#         #     writer = csv.DictWriter(file, fieldnames=field_names)
#         #
#         #     # Write the header
#         #     writer.writeheader()
#         #
#         #     # Write the data
#         #     writer.writerows(change_author_details)
#
#
# #
# # async def search_author_in_dblp(session, author):
# #     try:
# #         async with session.get(
# #             "https://dblp.org/search/author/api?q={}$&format=json&h=1000".format(quote(author['name']))
# #         ) as resp:
# #             author_data = await resp.json()
# #     except Exception as err:
# #         print(err)
# #         author_data = {}
# #
# #     hits = author_data.get("result", 0).get("hits", 0)
# #     no_of_hits = int(hits.get("@total", 0))
# #
# #     if no_of_hits > 0:
# #         print(author['name'])
# #         author_details = await get_author_detail_full_iteration(
# #             hits["hit"], disambiguated_authors=[author['name']]
# #         )
# #         print(author_details)
# #         await session.execute_write(
# #             update_author_dblp_info_by_id, aid=author['aid'], dblp_id=author_details['pid'] if author_details else None
# #         )
# #
# # async def search_authors_in_dblp(db):
# #     async with aiohttp.ClientSession() as session:
# #         with db.session(database=DB) as sync_session:
# #             # res = sync_session.execute_read(get_authors_not_found_on_dblp)
# #             # for closet data
# #             res = sync_session.execute_read(get_wrong_dblp_authors)
# #
# #             tasks = []
# #             for author_details in res:
# #                 author = author_details.data()['n']
# #                 task = search_author_in_dblp(session, author)
# #                 tasks.append(task)
# #
# #             await asyncio.gather(*tasks)

def get_name_match_list(db):
    with db.session(database=DB) as session:
        res = session.execute_read(get_wrong_matched_dblp_authors)
        auth_dic = {}

        for author_details in res:
            author = author_details.data()['n']
            auth_dic[author['name']] = []
            auth_nam = author['name']
            try:
                resp = requests.get("https://dblp.org/search/author/api?q={}&format=json".format(quote(author['name'])))
                author_data = resp.json()
            except Exception as err:
                print(err)
            hits = author_data.get("result", 0).get("hits", 0)
            no_of_hits = int(hits.get("@total", 0))
            print(hits)
            if no_of_hits > 0:
                for a in hits['hit']:
                    author_name = a.get("info").get("author")
                    auth_dic[auth_nam].append(author_name)

        with open("sample.json", "w") as outfile:
            for key, value in auth_dic.items():
                json.dump({key: value}, outfile)
                outfile.write('\n')


def get_details_from_closet_data_json_save_to_neo(db):
    names = []
    with open('./data/closet_data.json', 'r') as j_file, db.session(database=DB) as session:
        data = json.load(j_file)
        for index, item in enumerate(data['network']['items']):
            names.append(item['label'])
            url = item["url"].split("/")
            pid = url[-2] + "/" + url[-1]
            pid = pid.replace('.html', '')
            session.execute_write(make_creator, item['label'], index, pid)


def parse_coauthors_xml_for_connection_building(session, coauthors_xml_tree: str, source_pid: str):
    root = ET.fromstring(coauthors_xml_tree)
    print(root)
    result = []
    for coauthor in root.findall("author"):
        result.append({
            "pid": coauthor.get("pid"),
            "count": coauthor.get("count"),
            "name": coauthor.text
        })
    sorted_result = sorted(result, key=lambda x: x['count'], reverse=True)
    print(sorted_result)
    final_author_pid = None
    final_author_count = None
    final_author_name = None
    for coauthor_info in sorted_result:
        count = coauthor_info.get('count')
        pid = coauthor_info.get('pid')
        name = coauthor_info.get('name')
        found = False
        if pid:
            xml_url = 'https://dblp.org/pid/{}.xml'.format(quote(pid))
            try:
                response = requests.get(xml_url)
                if response.status_code == 200:
                    xml_dict = xmltodict.parse(response.content)
                    for x in xml_dict['dblpperson']['r']:
                        if isinstance(x, str):
                            continue
                        journal = ''
                        if x.get('article'):
                            journal = x['article']['booktitle'] if x['article'].get('booktitle') else x['article'][
                                'journal']
                        elif x.get('inproceedings'):
                            journal = x['inproceedings']['booktitle'] if x['inproceedings'].get('booktitle') else \
                                x['inproceedings']['journal']
                        elif x.get('proceedings'):
                            journal = x['proceedings']['booktitle'] if x['proceedings'].get('booktitle') else \
                                x['proceedings']['publisher']
                        elif x.get('incollection'):
                            journal = x['incollection']['booktitle'] if x['incollection'].get('booktitle') else \
                                x['incollection']['journal']

                        if journal in human_computer_interaction:
                            found = True
                            break
                else:
                    print(f'Failed to retrieve data. Status code: {response.status_code}')

            except requests.exceptions.RequestException as e:
                print(f'Error: {e}')
            except xmltodict.expat.ExpatError as e:
                print(f'Error parsing XML: {e}')
        if found:
            final_author_pid = pid
            final_author_count = count
            final_author_name = name
            break
    return {"final_author_count": final_author_count, "final_author_pid": final_author_pid,
            "final_author_name": final_author_name}


def check_coauthor_match_found_in_xml(coauthors_xml_tree, match_pid):
    root = ET.fromstring(coauthors_xml_tree)
    print(root)
    for coauthor in root.findall("author"):
        if coauthor.get('pid') == match_pid:
            return coauthor.get('count')
    return None


def connect_unconnected_relation_authors(db):
    print("[INFO]: Connecting unconnected authors by 1 hop relations in graph...")
    with db.session(database=DB) as session:
        author_pids = session.execute_read(get_wrong_matched_dblp_authors)
        # author_pids = ['93/1485', 'r/HeriRamampiaro', '50/2015']
        # print(author_pids)
        mutable_author_pid_list = author_pids
        for source_pid in tqdm(mutable_author_pid_list):
            source_pid = source_pid.data()['p']
            print(source_pid)
            try:
                coauthors_xml_tree = call_dblp_coauthor_api(source_pid)
                final_author = parse_coauthors_xml_for_connection_building(session, coauthors_xml_tree, source_pid)
                final_author_pid = final_author.get('final_author_pid')
                final_author_count = final_author.get('final_author_count')
                final_author_name = final_author.get('final_author_name')
                # Find if an author found
                if final_author_pid and final_author_count:
                    index = session.execute_read(get_max_aid)
                    print(index)
                    author_details = {
                        'aid': int(index[0])+1,
                        'name': final_author_name,
                        'flag_mutable': True,
                        'pid': final_author_pid
                    }
                    print("Final Details",author_details)
                    # Create new author
                    session.execute_write(make_author_mutable, author_details)
                    # Build relation
                    session.execute_write(make_relation, source_pid, final_author_pid, final_author_count)

                    # If found find the author as coauthor for all the people left
                    for mutable_author in author_pids:
                        mutable_author = mutable_author.data()['p']
                        print(mutable_author, "I am mutable .......................................")
                        if mutable_author == source_pid or mutable_author not in mutable_author_pid_list:
                            continue
                        mutable_coauthors_xml_tree = call_dblp_coauthor_api(mutable_author)
                        # If any author match found
                        if count := check_coauthor_match_found_in_xml(mutable_coauthors_xml_tree, final_author_pid):
                            print(mutable_author, final_author, count)
                            session.execute_write(make_relation, mutable_author, final_author_pid, count)
                            # Remove that author from mutable list
                            mutable_author_pid_list.remove(mutable_author)
            except ET.ParseError as errp:
                print(errp)
            except Exception as err:
                print(err)


def feed_json_resolved_blp_id(db):
    with db.session(database=DB) as session:
        file_path = 'your_file.json'
        with open(file_path, 'r') as file:
            data = json.load(file)
            print(data)
        for key, value in data.items():
            for item in value:
                pid = item['pid']
                session.execute_write(update_author_dblp_info_by_name, name=key, dblp_id=pid)


def produce_csv_for_unconnected_people(db):
    with db.session(database=DB) as session:
        authors = session.execute_read(get_unconnected_people)
        final_list=[]
        for author_details in authors:
            author = author_details.data()['n']
            if author.get('dblp_id'):
                author['dblp_url']=f"https://dblp.org/pid/{author['dblp_id']}.html"
            final_list.append(author)
        field_names = set()
        for entry in final_list:
            field_names.update(entry.keys())
        csv_file='list_of_unconnected_people.csv'
        # Writing to CSV file
        with open(csv_file, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=field_names)

            # Write the header
            writer.writeheader()

            # Write the data
            writer.writerows(final_list)

        print(f'Data has been written to {csv_file}')



