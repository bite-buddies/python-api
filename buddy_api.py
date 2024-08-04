import pandas as pd
import requests
from bson import json_util
from flask import Flask, request, jsonify
from pymongo import MongoClient
import json

app = Flask(__name__)

# Configure MongoDB client
client = MongoClient('mongodb://localhost:27017/')
db = client['user_db']  # Database name
restaurant_collection = db['restaurants']
buddies_collection = db['buddies']
reviews_collection = db['reviews']


def create_all_data(reviews_df, buddy_df):
    all_data = pd.merge(reviews_df, buddy_df, on="user_id")
    return all_data


def get_friends_and_fof(user_id, buddy_df):
    friends_series = buddy_df[buddy_df['user_id'] == user_id]['buddies']

    if friends_series.empty:
        return set()

    friends = set(friends_series.values[0])
    friends_of_friends = set()

    for friend_id in friends:
        temp_friends_series = buddy_df[buddy_df['user_id'] == friend_id]['buddies']

        if not temp_friends_series.empty:
            temp_friends = set(temp_friends_series.values[0])
            friends_of_friends.update(temp_friends)

    friends_of_friends.discard(user_id)
    friends_of_friends.difference_update(friends)

    relevant_friends = friends.union(friends_of_friends)

    return relevant_friends


def get_restraunt_rec(restaurant_df, user_id, buddy_df):
    all_data = create_all_data(restaurant_df, buddy_df)
    relevant_friends = get_friends_and_fof(user_id, buddy_df)
    print(relevant_friends)
    friend_restraunt_df = all_data[all_data['user_id'].isin(relevant_friends)]
    restraunt_tried = all_data[all_data['user_id'] == user_id]['rest_id']
    rec_df = friend_restraunt_df[~friend_restraunt_df['rest_id'].isin(restraunt_tried)]
    print(rec_df)
    rec = {}
    for restraunt in rec_df['rest_id']:
        if restraunt not in rec:
            ratings = rec_df[rec_df['rest_id'] == restraunt]['rating']
            rating = ratings.mean().round()
            if rating > 3:
                rec[restraunt] = rating
    return rec


def reccomend_dish(restaurant, user_id, buddy_df, restaurant_df):
    all_data = create_all_data(restaurant_df, buddy_df)
    relevant_friends = get_friends_and_fof(user_id, buddy_df)
    friends_df = all_data[all_data['user_id'].isin(relevant_friends)]
    restaurant_data = friends_df[friends_df['rest_id'] == restaurant]
    dish_tried = all_data[(all_data['user_id'] == user_id) & (all_data['rest_id'] == restaurant)]['dish_tried']
    dishes_to_rec = restaurant_data[~restaurant_data['dish_tried'].isin(dish_tried)]
    return dishes_to_rec


@app.route('/get_recommendations', methods=['GET'])
def get_recommendations():
    user_id = request.args.get('user_id')
    rest_id = request.args.get('rest_id')
    documents = restaurant_collection.find()
    documents_list = list(documents)
    rest_df = pd.DataFrame(documents_list)
    if '_id' in rest_df.columns:
        rest_df = rest_df.drop(columns=['_id'])

    documents = buddies_collection.find()
    documents_list = list(documents)
    buddy_df = pd.DataFrame(documents_list)
    if '_id' in buddy_df.columns:
        buddy_df = buddy_df.drop(columns=['_id'])

    documents = reviews_collection.find()
    documents_list = list(documents)
    reviews_df = pd.DataFrame(documents_list)
    if '_id' in reviews_df.columns:
        reviews_df = reviews_df.drop(columns=['_id'])

    print(rest_df)
    print(buddy_df)
    print(reviews_df)

    rest_rec = reccomend_dish(int(rest_id), int(user_id), buddy_df, reviews_df)
    rest_rec= rest_rec.drop(columns=['rest_id', 'user_id','buddies'])

    return rest_rec.to_json(orient='records')

@app.route('/get_reviews', methods=['GET'])
def get_reviews():
    user_id = request.args.get('user_id')
    rest_id = request.args.get('rest_id')

    if not user_id:
        return jsonify({'error': 'Please provide user_id parameter'}), 400

    if not rest_id:
        return jsonify({'error': 'Please provide rest_id parameter'}), 400

    try:
        user_id = int(user_id)
        rest_id = int(rest_id)
    except ValueError:
        return jsonify({'error': 'user_id and rest_id should be integers'}), 400

    # Query for the document with the specific user_id
    query = {'user_id': user_id}  # Ensure user_id is treated as an integer
    document = buddies_collection.find_one(query, {'buddies': 1, '_id': 0})  # Fetch only the buddies field

    if not document or 'buddies' not in document:
        return jsonify({'error': 'User not found or no buddies listed'}), 404

    buddies_list = document['buddies']
    buddies_list.append(int(user_id))
    print("Buddies List:", buddies_list)

    # Convert buddy_id values to strings

    # Query for reviews with the specific rest_id and buddy_id in buddies_list
    query = {'user_id': {'$in': buddies_list}, 'rest_id': rest_id}
    buddy_reviews = reviews_collection.find(query)

    # Convert query results to list of dictionaries
    reviews_list = list(buddy_reviews)

    if not reviews_list:
        return jsonify({'error': 'No reviews found for the specified buddies and restaurant'}), 404

    # Add buddy names to reviews and remove unwanted fields
    for review in reviews_list:
        buddy_id = review['user_id']
        buddy = buddies_collection.find_one({'user_id': int(buddy_id)}, {'name': 1, '_id': 0})
        if buddy:
            review['buddy_name'] = buddy['name']
        # Remove unwanted fields
        review.pop('user_id', None)
        review.pop('rest_id', None)
        review.pop('_id', None)

    # Convert list of dictionaries to JSON
    result_json = json_util.dumps(reviews_list)

    return result_json, 200

@app.route('/post_review', methods=['POST'])
def post_review():
    buddy_id = request.args.get('buddy_id')
    rest_id = request.args.get('rest_id')
    dish_name = request.args.get('dish_name')
    rating = request.args.get('rating')

    if buddy_id is None or dish_name is None or rating is None or rest_id is None:
        return jsonify({'error': 'Please provide id, name and rating parameters'}), 400

    review = {
        'buddy_id': buddy_id,
        'rest_id': rest_id,
        'dish_name': dish_name,
        'rating': rating
    }

    reviews_collection.insert_one(review)

    return jsonify({'result': 'Review added successfully'})


@app.route('/get_restaurants', methods=['GET'])
def get_restaurants():
    longitude = request.args.get('longitude')
    latitude = request.args.get('latitude')

    if longitude is None or latitude is None:
        return jsonify({'error': 'Please provide both longitude and latitude parameters'}), 400

    json_data = search_restaurants(longitude, latitude)

    df = pd.json_normalize(json_data)
    columns_dont_need = ['phone', 'display_phone', 'distance', 'business_hours', 'location.address1',
                         'location.address2', 'location.address3', 'id',
                         'alias', 'is_closed', 'url', 'review_count', 'categories', 'transactions', 'location.city',
                         'location.zip_code', 'location.state', 'location.country',
                         'attributes.business_temp_closed', 'attributes.menu_url', 'attributes.open24_hours',
                         'attributes.waitlist_reservation', 'price']
    restaurant_df = df.drop(columns=columns_dont_need)
    restaurant_df['rating'] = (restaurant_df['rating'] * 2).round()
    restaurant_df['location.display_address'] = restaurant_df['location.display_address'].apply(
        lambda lst: ' '.join(lst))
    restaurant_df['rest_id'] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    restaurant_df = restaurant_df.rename(columns={
        "coordinates.latitude": "latitude",
        "coordinates.longitude": "longitude",
        "location.display_address": "address"
    })
    restaurant_json = restaurant_df.to_json(orient="records")

    restaurant_collection.delete_many({})

    # Insert the restaurant data into the MongoDB collection
    restaurant_collection.insert_many(restaurant_df.to_dict('records'))

    return restaurant_json


def search_restaurants(long, lat,
                       api_key='urMqqB2UYH3ldFRClbTEeSmpBzLSmeHPDD_GFkbiwWAZTod8fDbM0WIxzqitOucIF5hGrVFUrNcEbofwWQFbwgWDrS2-yTiNAopwXnDljoaNfNo10DlUAfHBpVuuZnYx',
                       radius=15):
    radius_meters = radius * 1609.34

    headers = {
        'Authorization': f'Bearer {api_key}',
    }

    params = {
        'term': 'lunch',
        'latitude': lat,
        'longitude': long,
        'radius': int(radius_meters),
        'limit': 20  # Maximum number of results per request (max 50)
    }

    response = requests.get('https://api.yelp.com/v3/businesses/search', headers=headers, params=params)

    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        print(response.json())
        return []

    return response.json()['businesses']


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5006, debug=True)
