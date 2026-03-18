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


# In[36]:


df.to_csv("adelaide_cafes_dataset.csv", index=False)


# In[37]:


import pandas as pd
import numpy as np

df = pd.read_csv("adelaide_cafes_dataset.csv")

df.head()


# In[4]:


len(df)


# In[5]:


suburb_summary = df.groupby("suburb").agg(
    cafe_count=("name","count"),
    avg_rating=("rating","mean"),
    median_reviews=("user_ratings_total","median")
).reset_index()

suburb_summary = suburb_summary.sort_values(
    by="cafe_count",
    ascending=False
)

suburb_summary.head(10)


# In[10]:


top_review_cafes = df.sort_values(
    by="user_ratings_total",
    ascending=False
)

top_review_cafes[[
    "name",
    "suburb",
    "rating",
    "user_ratings_total"
]].head(20)


# In[7]:


df["review_score"] = df["user_ratings_total"] / df["user_ratings_total"].max()

df["rating_score"] = df["rating"] / 5


# In[8]:


df["website_score"] = df["website"].notna().astype(int)


# In[9]:


df["independent_score"] = df["independent_flag"].map({
    "Y":1,
    "N":0,
    "Unknown":0.5
})


# In[11]:


df = df.merge(
    suburb_summary[["suburb","cafe_count"]],
    on="suburb",
    how="left"
)

df["density_score"] = df["cafe_count"] / df["cafe_count"].max()


# In[12]:


df["acquisition_score"] = (
    df["review_score"] * 0.40 +
    df["rating_score"] * 0.25 +
    df["website_score"] * 0.10 +
    df["independent_score"] * 0.15 +
    df["density_score"] * 0.10
)


# In[13]:


priority_cafes = df.sort_values(
    by="acquisition_score",
    ascending=False
)


# In[14]:


top_targets = priority_cafes.head(30)

top_targets[[
    "name",
    "suburb",
    "rating",
    "user_ratings_total",
    "website",
    "acquisition_score"
]]


# In[15]:


chains = [
"Starbucks",
"Gloria Jean",
"McCafe",
"Cibo Espresso",
"Coffee Club",
"Hudsons Coffee"
]

def detect_chain(name):

    for c in chains:
        if c.lower() in str(name).lower():
            return "N"

    return "Y"

df["independent_flag"] = df["name"].apply(detect_chain)


# In[16]:


top100 = priority_cafes.head(100)


# In[17]:


suburb_summary["score"] = (
    suburb_summary["cafe_count"] * 0.5 +
    suburb_summary["avg_rating"] * 10 +
    suburb_summary["median_reviews"] * 0.05
)

top_suburbs = suburb_summary.sort_values(
    by="score",
    ascending=False
).head(5)


# In[18]:


priority_cafes.to_csv("rew_oz_priority_cafes.csv", index=False)

suburb_summary.to_csv("rew_oz_suburb_summary.csv", index=False)

top_targets.to_csv("rew_oz_top_30_targets.csv", index=False)


# In[38]:


import datetime

reviews = result.get("reviews", [])

first_review_date = None

if reviews:
    timestamps = [r["time"] for r in reviews]
    earliest = min(timestamps)

    first_review_date = datetime.datetime.fromtimestamp(earliest)

year = first_review_date.year if first_review_date else None
month = first_review_date.month if first_review_date else None


# In[39]:


recent_cafes = df[df["opening_year"] >= 2024]


# In[40]:


recent_cafes = df[df["opening_year"] == 2025]


# In[41]:


recent_cafes = df.sort_values(
    by="opening_year",
    ascending=False
)

recent_cafes.to_csv("recent_adelaide_cafes.csv", index=False)


# In[42]:


recent_summary = recent_cafes.groupby("suburb").size()

recent_summary.sort_values(ascending=False).head(10)


# In[43]:


print("Total cafes:", len(df))


# In[44]:


df["first_review_date"] = pd.to_datetime(df["first_review_date"])


# In[45]:


recent_cafes = df[
    df["first_review_date"] >= pd.Timestamp.today() - pd.DateOffset(months=12)
]


# In[46]:


print("Recent cafes:", len(recent_cafes))


# In[47]:


recent_cafes = recent_cafes.sort_values(
    by="first_review_date",
    ascending=False
)

recent_cafes.head(20)


# In[48]:


recent_cafes.to_csv("recent_adelaide_cafes.csv", index=False)


# In[49]:


recent_suburb_summary = (
    recent_cafes.groupby("suburb")
    .size()
    .reset_index(name="new_cafe_count")
    .sort_values(by="new_cafe_count", ascending=False)
)

recent_suburb_summary


# In[50]:


recent_cafes["year_month"] = recent_cafes["first_review_date"].dt.to_period("M")

monthly_trend = (
    recent_cafes.groupby("year_month")
    .size()
    .reset_index(name="new_cafes")
)

monthly_trend


# In[51]:


import matplotlib.pyplot as plt

plt.figure(figsize=(10,5))

plt.plot(monthly_trend["year_month"].astype(str),
         monthly_trend["new_cafes"],
         marker="o")

plt.title("New Cafe Openings per Month")
plt.xlabel("Month")
plt.ylabel("Number of Cafes")

plt.xticks(rotation=45)

plt.show()


# In[52]:


top_new_cafes = recent_cafes.sort_values(
    by="user_ratings_total",
    ascending=False
)

top_new_cafes[
    ["name","suburb","rating","user_ratings_total","first_review_date"]
].head(15)


# In[ ]:


df.to_csv("adelaide_cafes_dataset.csv", index=False)

