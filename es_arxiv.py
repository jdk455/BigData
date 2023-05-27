# Import the Elasticsearch client library and the requests library
from elasticsearch import Elasticsearch
import requests
import xml.etree.ElementTree as ET

# Create an Elasticsearch instance
es = Elasticsearch()

# Define a function to index papers from arXiv
def index_papers(query, size):
# Create an index for papers
    es.indices.create(index='papers', ignore=400)
    # Define the base URL for arXiv API
    base_url = 'http://export.arxiv.org/api/query?'
    # Define the parameters for arXiv API
    params = {
    'search_query': query, # The query string
    'start': 0, # The start index
    'max_results': size, # The maximum number of results
    'sortBy': 'lastUpdatedDate', # The sort criteria
    'sortOrder': 'descending' # The sort order
    }
    # Make a GET request to arXiv API
    response = requests.get(base_url, params=params)
    # Check if the request is successful
    if response.status_code == 200:
    # Parse the XML response
        root = ET.fromstring(response.text)
        # Iterate over the entry elements
        for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
        # Extract the paper id, title, summary, authors, and updated date
            paper_id = entry.find('{http://www.w3.org/2005/Atom}id').text.split('/')[-1]
            paper_title = entry.find('{http://www.w3.org/2005/Atom}title').text
            paper_summary = entry.find('{http://www.w3.org/2005/Atom}summary').text
            paper_authors = [author.find('{http://www.w3.org/2005/Atom}name').text for author in entry.findall('{http://www.w3.org/2005/Atom}author')]
            paper_updated = entry.find('{http://www.w3.org/2005/Atom}updated').text
            print(paper_title)
            # Index the paper document using the paper id as the document id
            es.index(index='papers', id=paper_id, body={
            'title': paper_title,
            'summary': paper_summary,
            'authors': paper_authors,
            'updated': paper_updated
            })
        # Return the number of indexed papers
        return len(root.findall('{http://www.w3.org/2005/Atom}entry'))
    else:
        # Return an error message
        return f'Error: {response.status_code}'

# Test the function with some example queries and sizes
print(index_papers('machine learning', 10))
print(index_papers('natural language processing', 20))




# Connect to Neo4j
uri = "bolt://localhost:7687"
user = "neo4j"
password = "password"
driver = GraphDatabase.driver(uri, auth=(user, password))

# Define a Cypher query to create the nodes and relationships in Neo4j
cypher_query = """
UNWIND $papers AS paper
MERGE (p:Paper {id: paper.id})
SET p.title = paper.title,
    p.summary = paper.summary,
    p.authors = paper.authors,
    p.updated = paper.updated
"""

# Define a function to extract papers from Elasticsearch and load them into Neo4j
def index_papers_to_neo4j(query, size):
    # Search for papers in Elasticsearch
    response = es.search(index='papers', q=query, size=size)
    papers = response['hits']['hits']
    # Transform the paper documents into dictionaries
    paper_dicts = []
    for paper in papers:
        paper_dict = paper['_source']
        paper_dict['id'] = paper['_id']
        paper_dicts.append(paper_dict)
   
    # Load the paper dictionaries into Neo4j
    with driver.session() as session:
        session.run(cypher_query, papers=paper_dicts)

# Call the function to index papers with the "machine learning" query and a size of 10
index_papers_to_neo4j('machine learning', 10)
# Call the function to index papers with the "natural language processing" query and a size of 20
index_papers_to_neo4j('natural language processing', 20)