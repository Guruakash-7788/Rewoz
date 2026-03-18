#!/usr/bin/env python
# coding: utf-8

# In[35]:


#!/usr/bin/env python
# coding: utf-8

import os
import requests
import pandas as pd
import time
import datetime

API_KEY = os.getenv("GOOGLE_API_KEY")

if not API_KEY:
    raise ValueError("API key not found! Set GOOGLE_API_KEY in environment.")


grid_points = [
    (-34.9285, 138.6007),  # CBD
    (-34.9800, 138.5150),  # Glenelg
    (-34.8840, 138.5940),  # Prospect
    (-34.9510, 138.6070),  # Unley
    (-34.9070, 138.5960),  # North Adelaide
    (-34.9210, 138.6300),  # Norwood
    (-34.9180, 138.4940),  # Henley Beach
    (-34.8280, 138.6870),  # Tea Tree Gully
]

all_place_ids = set()

print("Collecting cafes...\n")

for lat, lng in grid_points:

    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"

    params = {
        "location": f"{lat},{lng}",
        "radius": 5000,
        "type": "cafe",
        "keyword": "cafe",
        "key": API_KEY
    }

    while True:

        response = requests.get(url, params=params)
        data = response.json()

        print("Nearby Search Status:", data.get("status"))

        if data.get("status") != "OK":
            break

        for result in data.get("results", []):
            all_place_ids.add(result["place_id"])

        next_page = data.get("next_page_token")

        if not next_page:
            break

        time.sleep(2)

        params = {
            "pagetoken": next_page,
            "key": API_KEY
        }

print("\nTotal cafes discovered:", len(all_place_ids))


rows = []

for i, place_id in enumerate(all_place_ids):

    print(f"Fetching details {i+1}/{len(all_place_ids)}")

    url = "https://maps.googleapis.com/maps/api/place/details/json"

    params = {
        "place_id": place_id,
        "fields": "name,formatted_address,address_components,geometry,formatted_phone_number,website,rating,user_ratings_total,price_level,opening_hours,types,reviews",
        "key": API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()

    status = data.get("status")
    print("Details status:", status)

    if status != "OK":
        time.sleep(2)
        continue

    result = data.get("result", {})

    name = result.get("name")
    address = result.get("formatted_address")

    lat = result.get("geometry", {}).get("location", {}).get("lat")
    lng = result.get("geometry", {}).get("location", {}).get("lng")

    phone = result.get("formatted_phone_number")
    website = result.get("website")

    rating = result.get("rating")
    review_count = result.get("user_ratings_total")
    price_level = result.get("price_level")

    types = ", ".join(result.get("types", []))

    suburb = None
    postcode = None
    state = None

    for comp in result.get("address_components", []):

        if "locality" in comp["types"]:
            suburb = comp["long_name"]

        if "postal_code" in comp["types"]:
            postcode = comp["long_name"]

        if "administrative_area_level_1" in comp["types"]:
            state = comp["short_name"]

    opening_hours = None

    if "opening_hours" in result:
        opening_hours = " | ".join(
            result["opening_hours"].get("weekday_text", [])
        )

    first_review_date = None
    opening_year = None
    opening_month = None

    reviews = result.get("reviews", [])

    if reviews:

        timestamps = [r["time"] for r in reviews]
        earliest = min(timestamps)

        first_review_date = datetime.datetime.fromtimestamp(earliest)

        opening_year = first_review_date.year
        opening_month = first_review_date.month


    google_maps_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"

    independent_flag = "Unknown"
    pos_hint = "Unknown"
    notes = ""

    rows.append([
        name,
        place_id,
        address,
        suburb,
        postcode,
        state,
        lat,
        lng,
        phone,
        website,
        google_maps_url,
        rating,
        review_count,
        price_level,
        opening_hours,
        types,
        opening_year,
        opening_month,
        first_review_date,
        independent_flag,
        pos_hint,
        notes
    ])

    time.sleep(2)

print("\nRows collected:", len(rows))


columns = [
    "name",
    "place_id",
    "address",
    "suburb",
    "postcode",
    "state",
    "lat",
    "lng",
    "phone",
    "website",
    "google_maps_url",
    "rating",
    "user_ratings_total",
    "price_level",
    "opening_hours",
    "types",
    "opening_year",
    "opening_month",
    "first_review_date",
    "independent_flag",
    "pos_hint",
    "notes"
]

df = pd.DataFrame(rows, columns=columns)

df.head(20)

df.to_csv("adelaide_cafes_dataset.csv", index=False)







df.to_csv("adelaide_cafes_dataset.csv", index=False)

