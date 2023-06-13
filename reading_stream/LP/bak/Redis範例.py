import json

import redis
from redis.commands.search.aggregation import AggregateRequest, Asc
from redis.commands.search.field import NumericField, TextField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query

pool = redis.ConnectionPool(host="172.17.0.2", port=6379, db=0)
r = redis.Redis(connection_pool=pool)

# Search the index for a string
search_result = r.ft("filelog_idx").search(
    Query("@raw_file:(LP*)").return_field("$.file_seqno", as_field="seqno")
)

for doc in search_result.docs:
    print(doc["id"], doc["seqno"])
    # .return_field('file_seqno')


# Search the index for a string
search_result = r.ft("maindata_idx").search(
    Query("@meter:{12345678}")
    .return_field("$.sdp_id", as_field="sdp_id")
    .return_field("$.data[?(@.begin<=1550628920&&@.end>1550628920)]", as_field="data")
)

for doc in search_result.docs:
    print(doc["data"], type(doc["sdp_id"]))
    # print(doc["data"], type(doc["data"]))

    res = json.loads(doc["data"])
    print("start_date_time:", res["start_date_time"])
    print("end_date_time:", res["end_date_time"])
    print("tou_id:", res["tou_id"])


def Convert(lst):
    res_dct = {
        lst[i].decode("utf-8"): lst[i + 1].decode("utf-8")
        for i in range(0, len(lst), 2)
    }
    return res_dct


# ft.aggregate filelog_idx2 * GROUPBY 1 @raw_file REDUCE MAX 1 @seqno as seqno
myobj = r.execute_command(
    "ft.aggregate",
    "filelog_idx2",
    "*",
    "GROUPBY",
    "1",
    "@raw_file",
    "REDUCE",
    "MAX",
    "1",
    "@seqno",
    "as",
    "seqno",
)
o = Convert(myobj[1])
max_seqno = o["seqno"]
print("filelog max seqno:", max_seqno)

# ft.aggregate filelog_idx "@raw_file:{varSrchKey}" GROUPBY 1 @raw_file REDUCE MAX 1 @seqno as seqno
# ft.aggregate filelog_idx2 "@raw_file:{LP_1cbd57c9\\-1ddc\\-42c2\\-bb22\\-07da17f6ad92_20221118084235*}" GROUPBY 1 @raw_file REDUCE MAX 1 @seqno as seqno
myobj = r.execute_command(
    "ft.aggregate",
    "filelog_idx2",
    "@raw_file:{LP_1cbd57c9\\-1ddc\\-42c2\\-bb22\\-07da17f6ad92_20221118084235*}",
    "GROUPBY",
    "1",
    "@raw_file",
    "REDUCE",
    "MAX",
    "1",
    "@seqno",
    "as",
    "seqno",
)
o = Convert(myobj[1])
max_seqno = o["seqno"]
print("filelog max seqno:", max_seqno)
