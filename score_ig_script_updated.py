##Importations des libraires
from pymongo import MongoClient
from scipy.spatial import distance
import config


##Connection a mongo
cluster = MongoClient(config.MONGO_PASSPHRASE)
base = cluster[config.MONGO_DB_NAME]
page_ig_stat = base[config.COLL_IG_STATS]
page_ig = base[config.COLL_IG]
page_ig_media = base[config.COLL_IG_MEDIA]
inluence_account = base[config.COLL_INFLUENCE_ACCOUNTS]

##
base.media.drop()

##Division
division = [10000000,5000,200,100,50,0]
#division = [0, 50, 100, 500, 5000, 1000000]
for result in page_ig.find():
    for i in range(len(division)-1):
        if result["followers_count"] <= division[i] and result["followers_count"] > division[i+1]:
            page_ig.update_one({"_id": result['_id']}, {"$set": {"division": i+1}})
    if result["followers_count"] < division[len(division) - 1]:
        page_ig.update_one({"_id": result['_id']}, {"$set": {"division": len(division)}})

##Traitement de la base page_media
base = cluster['page_insta']
media = base["media"]

##Injection resultat dans la base media
agg_result = page_ig_media.aggregate(
    [{
        "$group":
            {
                "_id": "$id",
                "date": {"$max": "$date"},
                "owner": {"$max": "$owner"},
                "impressions": {"$sum": "$impressions"},
                "reach": {"$sum": "$reach"},
                "engagement": {"$sum": "$engagement"}

            }}
    ])
for i in agg_result:
    media.insert_many(agg_result, ordered=False)

##Ajouter le taux d'engagement par post
for result in media.find():
    try:
        reach=result["reach"]
        engagement=result["engagement"]
        Tx_EG_Post = engagement / reach
        media.update_one({"_id": result['_id']}, {"$set": {"Taux_EG_post": Tx_EG_Post}})
    except:
        media.update_one({"_id": result['_id']}, {"$set": {"Taux_EG_post": 0}})

##Inserer le avg_eg dans la page instagram
agg= media.aggregate(
    [{
    "$group" :
        {"_id" : "$_id",
         "_id" : "$owner",
         "date": { "$max": "$date" },
         "owner": { "$max": "$owner" },
         "nb_posts" : {"$sum" :1},
         "Taux_EG_post" : {"$sum" : "$Taux_EG_post"}
         }}
    ])
for i in agg:
    #print(i)
    try:
        Tx_EG_post=i['Taux_EG_post']
        nb_post=i['nb_posts']
        page_ig.update_one({"id": i["owner"]}, {"$set":{'Sum_Eg_Posts':Tx_EG_post }})
        page_ig.update_one({"id": i["owner"]}, {"$set": {'NB_posts': nb_post}})
        page_ig.update_one({'_id': i['_id']}, {'$pull': {'owner': None}})
    except:
        page_ig.update_one({"id": i["owner"]}, {"$set": {'Sum_Eg_Posts': 0}})
        page_ig.update_one({"id": i["owner"]}, {"$set": {'NB_posts': 0}})

##Jointure page_ig avec page_stat
for result in page_ig.find():
    for doc in page_ig_stat.find():
        if doc["id"] == result["page_id"]:
                reach=doc["reach"]
                impressions=doc['impressions']
                profile_views=doc['profile_views']
    page_ig.update_one({"_id": result['_id']}, {"$set": {"profile_reach": reach,"profile_impressions":impressions,"profile_views":profile_views}})

##Calculer avg_par le nombre de posts ( moyenne des posts )
for res in page_ig.find():
    try:
        Eg_Posts = res["Sum_Eg_Posts"]
        posts = res["NB_posts"]
        avg = Eg_Posts / posts
        page_ig.update_one({"_id": res['_id']}, {"$set": {"AVG_EG_POSTS": avg}})
    except:
        page_ig.update_one({"_id": res['_id']}, {"$set": {"AVG_EG_POSTS": 0}})

##Calcule l'indice de chevauchement
    page_ig.update_one({"_id": res['_id']},
                   {"$set": {"FOLLOW_RATE": res["followers_count"] / res["follows_count"]}})

##Calcule le reach_rate
for result in page_ig.find():
    try:
        followers = result["followers_count"]
        reach = result["profile_reach"]
        reach_rate = reach / followers
        page_ig.update_one({"_id": result['_id']}, {"$set": {"REACH_RATE": reach_rate}})
    except:
        page_ig.update_one({"_id": result['_id']}, {"$set": {"REACH_RATE": 0}})

## Normamlization
division_div =[1,2,3,4,5,6]
instagram_features = [ "followers_count", "FOLLOW_RATE" ,"REACH_RATE" , "AVG_EG_POSTS", "profile_views"]
for feature in instagram_features:
    #print(feature)
    for div in division_div:
        #print(div)
        pipeline = [
            {"$match": {"division": div}},
            {"$group": {
                "_id": "_id",
                "max": {"$max": "$" + feature},
                "min": {"$min": "$" + feature}
            }
            }
        ];
        res = page_ig.aggregate(pipeline)
        for i in res:
            #print(i)
            if len(list(page_ig.aggregate(pipeline))) != 0:
                min = list(page_ig.aggregate(pipeline))[0]['min']
                #print(min)
                max = list(page_ig.aggregate(pipeline))[0]['max']
                #print(max)
                for user in page_ig.find({"division": div}):
                    if (max - min) != 0:
                        page_ig.update_one({"_id": user['_id']},{"$set": {feature + "_normal": (user[feature] - min) / (max - min)}})
                    #else:
                        #page_ig.update_one({"_id": user['_id']}, {"$set": {feature + "_normal": 0}})
##
base.media.drop()
#### Implémentation de TOPSIS
feature_list = [ "followers_count_normal", "FOLLOW_RATE_normal" ,"REACH_RATE_normal" , "AVG_EG_POSTS_normal", "profile_views_normal"]
best_i = [0.3,0.2,0.2,0.2,0.1]
worst_i = [0, 0, 0, 0, 0]
for user in page_ig.find():
    feature_list = []

    feature_list.append(0.3 * user["followers_count_normal"])
    feature_list.append(0.2 * user["FOLLOW_RATE_normal"])
    feature_list.append(0.2 * user["REACH_RATE_normal"])
    feature_list.append(0.2 * user["AVG_EG_POSTS_normal"])
    feature_list.append(0.1 * user["profile_views_normal"])

    s_moins = distance.euclidean(feature_list, worst_i)
    s_plus = distance.euclidean(feature_list, best_i)

    score = (s_moins / (s_moins + s_plus)) * 100

    page_ig.update_one({"_id": user['_id']}, {"$set": {"SCORE": int(score)}})
    inluence_account.update_one({"instagram": user["id"]}, {"$set": {"instagram_score": int(score)}})
    inluence_account.update_one({"instagram": user["id"]}, {"$set": {"instagram_division": user["division"]}})
    inluence_account.update_one({"instagram": user["id"]}, {"$set": {"instagram_followers": user["followers_count"]}})








