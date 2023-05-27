# Import the requests library and the ElementTree module
from datetime import datetime
from elasticsearch import Elasticsearch
import requests
import xml.etree.ElementTree as ET


# 1.介绍如何使用 Python 的 requests 库和 arXiv API 爬取论文的元数据和全文数据，并进行数据清洗和预处理
# Define a function to crawl papers from arXiv
def crawl_papers(query, size):
    # Define the base URL for arXiv API
    base_url = 'http://export.arxiv.org/api/query?'
    # Define the parameters for arXiv API
    params = {
        'search_query': query,  # The query string
        'start': 0,  # The start index
        'max_results': size,  # The maximum number of results
        'sortBy': 'lastUpdatedDate',  # The sort criteria
        'sortOrder': 'descending'  # The sort order
    }
    # Make a GET request to arXiv API
    response = requests.get(base_url, params=params)
    # Check if the request is successful
    if response.status_code == 200:
        # Parse the XML response
        root = ET.fromstring(response.text)
        # Initialize an empty list to store the paper data
        papers = []
        # Iterate over the entry elements
        for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
            # Extract the paper id, title, summary, authors, and updated date
            paper_id = entry.find(
                '{http://www.w3.org/2005/Atom}id').text.split('/')[-1]
            paper_title = entry.find('{http://www.w3.org/2005/Atom}title').text
            paper_summary = entry.find(
                '{http://www.w3.org/2005/Atom}summary').text
            paper_authors = [author.find(
                '{http://www.w3.org/2005/Atom}name').text for author in entry.findall('{http://www.w3.org/2005/Atom}author')]
            paper_updated = entry.find(
                '{http://www.w3.org/2005/Atom}updated').text
            # Append a dictionary of paper data to the list
            papers.append({
                'id': paper_id,
                'title': paper_title,
                'summary': paper_summary,
                'authors': paper_authors,
                'updated': paper_updated
            })
        # Return the list of paper data
        return papers
    else:
        # Return an error message
        return f'Error: {response.status_code}'


# 2.介绍如何使用 Python 的 elasticsearch 库和 ElasticSearch 进行论文数据的存储和索引，以及如何设置合适的映射和设置
# Import the Elasticsearch client library and the datetime module

# Create an Elasticsearch instance
es = Elasticsearch()

# Define a function to index papers to Elasticsearch


def index_papers(papers):
    # Create an index for papers with a custom mapping and settings
    es.indices.delete(index='papers')
    es.indices.create(index='papers', body={
        'mappings': {
            'properties': {
                'id': {'type': 'keyword'},
                'title': {'type': 'text', 'analyzer': 'english'},
                'summary': {'type': 'text', 'analyzer': 'english','fielddata': True},
                'authors': {'type': 'keyword'},
                'updated': {'type': 'date'}
            }
        },
        'settings': {
            'index': {
                'number_of_shards': 1,
                'number_of_replicas': 0
            }
        }
    }, ignore=400)
    # Iterate over the papers
    for paper in papers:
        # print(paper)
        # Index the paper document using the paper id as the document id and convert the updated date to ISO format
        es.index(index='papers', id=paper['id'], body={
            'id': paper['id'],
            'title': paper['title'],
            'summary': paper['summary'],
            'authors': paper['authors'],
            'updated': datetime.strptime(paper['updated'], '%Y-%m-%dT%H:%M:%SZ').isoformat()
        })

#######3.介绍如何使用 Python 的 elasticsearch-dsl 库和 ElasticSearch 进行论文数据的检索和挖掘，包括基于关键词、作者、主题等的精确匹配和模糊匹配，以及基于内容相似度、引用关系等的相关性排序和推荐
# Import the elasticsearch-dsl library and the Search class
from elasticsearch_dsl import Search

# Define a function to search papers from Elasticsearch based on various criteria and options
def search_papers(query=None, author=None, topic=None, similar_to=None, size=10):
    # Create a Search object using the Elasticsearch instance and the papers index
    s = Search(using=es, index='papers')
    # If query is given, add a match query on title and summary fields with default operator AND
    if query:
        s = s.query('multi_match', query=query, fields=['title', 'summary'], operator='and')
        # If author is given, add a term query on authors field with lowercased value
        if author:
            s = s.query('term', authors=author.lower())
            # If topic is given, add a match query on summary field with default operator OR and minimum should match 2
            if topic:
                s = s.query('match', summary={'query': topic, 'operator': 'or', 'minimum_should_match': 2})
                # If similar_to is given, add a more like this query on title and summary fields with like value and min term freq 1 
                if similar_to:
                    s = s.query('more_like_this', fields=['title', 'summary'], like=similar_to, min_term_freq=1)
                    # Set the size of the search results to the given size or default value 10 
                    s = s[:size]
                    # Execute the search and return the hits as a list of dictionaries 
    return [hit.to_dict() for hit in s.execute()]

#4.介绍如何使用 Python 的 matplotlib 库和 ElasticSearch 进行论文数据的可视化，包括论文数量、热门主题、作者合作网络等的统计图表和交互界面
# Import the matplotlib library and pyplot module 
import matplotlib.pyplot as plt

# Define a function to visualize papers from Elasticsearch based on various criteria and options 
def visualize_papers(query=None, author=None, topic=None, similar_to=None):

    # Create a Search object using the Elasticsearch instance and the papers index 
    s = Search(using=es, index='papers')

    # If query is given, add a match query on title and summary fields with default operator AND 
    if query:
        s = s.query('multi_match', query=query, fields=['title', 'summary'], operator='and')
        # s = s.query('multi_match', query=query, fields=['title'], operator='and')


        # If author is given, add a term query on authors field with lowercased value 
        if author:
            s = s.query('term', authors=author.lower())

            # If topic is given, add a match query on summary field with default operator OR and minimum should match 2 
            if topic:
                s = s.query('match', summary={'query': topic, 'operator': 'or', 'minimum_should_match': 2})

                # If similar_to is given, add a more like this query on title and summary fields with like value and min term freq 1 
                if similar_to:
                    s = s.query('more_like_this', fields=['title', 'summary'], like=similar_to, min_term_freq=1)

    # Add an aggregation on updated field with monthly interval and format yyyy-MM 
    s.aggs.bucket('papers_over_time', 'date_histogram', field='updated', calendar_interval='month', format='yyyy-MM')

    # Add an aggregation on summary field with significant terms and size 10 
    s.aggs.bucket('popular_topics', 'significant_terms', field='summary', size=10)

    # Add an aggregation on authors field with terms and size 10 
    s.aggs.bucket('top_authors', 'terms', field='authors', size=10)

    # Execute the search and get the aggregations as dictionaries 
    response = s.execute()
    papers_over_time = response.aggregations.papers_over_time.buckets 
    popular_topics = response.aggregations.popular_topics.buckets 
    top_authors = response.aggregations.top_authors.buckets 

    # Create a figure with three subplots 
    fig, (ax1, ax2, ax3) = plt.subplots(3)

    # # Plot the papers over time as a line chart on ax1 
    # ax1.bar([bucket.key for bucket in papers_over_time], [bucket.doc_count for bucket in papers_over_time])
    # ax1.set_xlabel('Month')
    # ax1.set_ylabel('Number of Papers')
    # ax1.set_title('Papers Over Time')
    # plt.show()

    # Plot the popular topics as a bar chart on ax2
    ax2.bar([bucket.key for bucket in popular_topics], [bucket.doc_count for bucket in popular_topics])
    ax2.set_xlabel('Topic')
    ax2.set_ylabel('Number of Papers')
    ax2.set_title('Popular Topics')
    ax2.tick_params(axis='x', rotation=45)

    # Plot the top authors as a pie chart on ax3
    ax3.pie([bucket.doc_count for bucket in top_authors], labels=[bucket.key for bucket in top_authors], autopct='%1.1f%%')
    ax3.set_title('Top Authors')

    # Adjust the layout and show the figure
    fig.tight_layout()
    plt.show()

#5.测试数据
# # Crawl 100 papers from arXiv with the query 'machine learning'
# papers = crawl_papers('machine learning', 100)

# # Index the papers to Elasticsearch
# index_papers(papers)

# # Search for papers with the query 'deep learning' and visualize the results
visualize_papers('machine learning')