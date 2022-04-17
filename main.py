# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from collections import defaultdict
import pandas as pd
import bisect
import json


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def member_group_add(member_id, group_id, join_time):
    if not group_id:
        return

    if member_id in member_group.keys():
        if group_id in member_group[member_id][0]:
            if join_time < member_group[member_id][1][member_group[member_id][0].index(group_id)]:
                member_group[member_id][1][member_group[member_id][0].index(group_id)] = join_time
            else:
                return
        else:
            bisect.insort(member_group[member_id][0], group_id)
            bisect.insort(member_group[member_id][1], join_time)
            return
    else:
        member_group[member_id] = [[group_id], [join_time]]
        return



def data_preprocess():
    # Join time: A.Group inti time   B. The first time to participate in an activity
    # Note: key: member_id  value: (group_id[], join_time[])
    data_event_host = pd.read_csv("./Data/eventhosts.csv")  # eventid, memberid
    data_events = pd.read_csv("./Data/event.csv",
                              error_bad_lines=False)  # id, name, event_url, lat, lon, country, city, groupid, created, visibility, time
    # data_member = pd.read_csv("./Data/member.csv") # id, name, link, lat, lon, country, city, hometown, joined, lang, visited
    data_group = pd.read_csv("./Data/group.csv", error_bad_lines=False ) # id,name,urlname,rating,categoryname,join_mode,lat,lon,country,city,created,members,organizer
    # data_topics = pd.read_csv("./Data/topics.csv") # id, name
    data_rsvp = pd.read_csv("./Data/rsvp.csv") # eventid, memberid, mtime
    # data_memertopics = pd.read_csv("./Data/membertopics.csv") # memberid, topicid
    global member_group
    member_group = defaultdict(list)
    # firstly create hash_map: event_id -> group id
    event_group = defaultdict(int)
    event_time = defaultdict(int)
    for index, row in data_events.iterrows():
        event_group[str(row['id'])] = int(row['groupid'].replace('"', '').replace(',', ''))
        event_time[str(row['id'])] = int(row['time'].replace('"', '').replace(',', ''))

    # event host is in group
    for index, row in data_event_host.iterrows():
        member_group_add(int(row['memberid']), event_group[str(row['eventid'])], event_time[str(row['eventid'])])

    for index, row in data_group.iterrows():
        member_group_add(int(row['organizer'].replace('"', '').replace(',', '')), row['id'], int(row['created'].replace('"', '').replace(',', '')))

    for index, row in data_rsvp.iterrows():
        member_group_add(int(row['memberid']), event_group[str(row['eventid'])], int(row['mtime']))

    js = json.dumps(member_group)
    file = open('./Data/member_group.txt', 'w')
    file.write(js)
    file.close()

    # 读取
    # file = open('test.txt', 'r')
    # js = file.read()
    # dic = json.loads(js)
    # print(dic)
    # file.close()

    return


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    data_preprocess()
    print_hi('PyCharm')
