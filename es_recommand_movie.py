# Import the Elasticsearch client library
from elasticsearch import Elasticsearch

# Create an Elasticsearch instance
es = Elasticsearch(['localhost:9200'])

# Define a function to index some sample data


def index_data():
    # Create an index for movies
    es.indices.create(index='movies', ignore=400)
    # Index some sample movie documents
    es.index(index='movies', id=1, body={
             'title': 'The Matrix', 'genres': ['Action', 'Sci-Fi'], 'rating': 8.7})
    es.index(index='movies', id=2, body={'title': 'The Lord of the Rings: The Fellowship of the Ring', 'genres': [
             'Adventure', 'Fantasy'], 'rating': 8.8})
    es.index(index='movies', id=3, body={
             'title': 'The Godfather', 'genres': ['Crime', 'Drama'], 'rating': 9.2})
    es.index(index='movies', id=4, body={
             'title': 'The Shawshank Redemption', 'genres': ['Drama'], 'rating': 9.3})
    es.index(index='movies', id=5, body={'title': 'The Dark Knight', 'genres': [
             'Action', 'Crime', 'Drama'], 'rating': 9.0})

    # Define a function to recommend movies based on a given movie title


def recommend_movies(title):
    # Query the movies index for the given title
    query = {
        'query': {
            'match': {
                'title': title
            }
        }
    }


    response = es.search(index='movies', body=query)
    # Get the first matching movie document
    movie = response['hits']['hits'][0]['_source']
    # Get the genres and rating of the movie
    genres = movie['genres']
    rating = movie['rating']
    # Query the movies index for movies with similar genres and rating
    query = {
        'query': {
            'bool': {
                'must': [
                    {
                        'terms': {
                            'genres.keyword': genres
                        }
                    },
                    {
                        'range': {
                            'rating': {
                                'gte': rating - 0.5,
                                'lte': rating + 0.5
                            }
                        }
                    }
                ]
            }
        }
    }
    response = es.search(index='movies', body=query)
    # Get the titles of the recommended movies
    recommendations = [hit['_source']['title'] for hit in response['hits']['hits']]
    # Return the recommendations
    return recommendations

# Index some sample data
index_data()
# Test the recommendation function with some examples
print(recommend_movies('The Matrix'))
print(recommend_movies('The Godfather'))
