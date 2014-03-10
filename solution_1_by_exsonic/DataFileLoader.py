'''
Created on 2013-4-4
@author: Bobi Pu, bobi.pu@usc.edu
'''

from datetime import datetime
from pymongo import MongoClient

class DataFileLoader(object):
    def __init__(self):
        self.db = MongoClient().EventRecommender
        
    def readCSVFile(self, CSVFileDir):
        with open(CSVFileDir) as f:
            lines = f.readlines()
        del lines[0]
        if not lines[-1]:
            del lines[-1]
        return lines
    
    def loadUsers(self, CSVFileDir):
        lines = self.readCSVFile(CSVFileDir)
        users = {}
        for line in lines:
            items = line.strip().split(',')
            user = {}
            user['id'] = items[0]
            user['language'] = items[1]
            user['gender'] = 1 if items[3] == 'male' else 0
            user['location'] = items[5] if items[5] != '' else None
            user['timeZone'] = items[6].strip()
            try:
                user['age'] = 2013 - int(items[2])
            except:
                user['age'] = None
            try:
                user['joinTime'] = datetime.strptime(items[4], '%Y-%m-%dT%H:%M:%S.%fZ')
            except:
                user['joinTime'] = None
            users[user['id']] = user
        return users
    
    def loadFriends(self, CSVFileDir, users):
        lines = self.readCSVFile(CSVFileDir)
        for line in lines:
            items = line.strip().split(',')
            users[items[0]]['friends'] = [friendId for friendId in items[1].split()]
        return users
    
    def insertUsersToDB(self, usersCSVFile, friendsCSVFile):
        users = self.loadUsers(usersCSVFile)
        users = self.loadFriends(friendsCSVFile, users)
        print 'Begin to insert users into MongoDB: EventRecommender' 
        for user in users.values():
            self.db.user.insert(user)
        print 'Done'
        
    def loadEvents(self, CSVFileDir):
        lines = self.readCSVFile(CSVFileDir)
        events = {}
        for line in lines:
            items = line.strip().split(',')
            event = {}
            event['id'] = items[0]
            event['hostUserId'] = items[1]
            event['startTime'] = datetime.strptime(items[2], '%Y-%m-%dT%H:%M:%S.%fZ') if items[2] else None
            event['city'] = items[3] if items[3] else None
            event['state'] = items[4] if items[4] else None
            event['zip'] = items[5] if items[5] else None
            event['country'] = items[6] if items[6] else None
            event['latitude'] = float(items[7]) if items[7] else None
            event['longitude'] = float(items[8]) if items[8] else None
            event['keywords'] = [int(item) for item in items[9:]]
            events[event['id']] = event
        return events
    
    def loadEventAttendees(self, CSVFileDir, events):
        lines = self.readCSVFile(CSVFileDir)
        for line in lines:
            try:
                items = line.strip().split(',')
                events[items[0]]['willAttendUsers'] = [userId for userId in items[1].split()] if items[1] else []
                events[items[0]]['mayAttendUsers'] = [userId for userId in items[2].split()] if items[2] else []
                events[items[0]]['notAttendUsers'] = [userId for userId in items[4].split()] if items[4] else []
                events[items[0]]['invitedUsers'] = [userId for userId in items[3].split()] if items[3] else []
            except Exception as e:
                #some event in event_attendees.csv but not in events.csv
                print e
                continue
        return events
    
    def insertEventsToDB(self, eventsCSVFile, attendeesCSVFile):
        events = self.loadEvents(eventsCSVFile)
        events = self.loadEventAttendees(attendeesCSVFile, events)
        print 'Begin to insert events into MongoDB: EventRecommender' 
        for event in events.values():
            self.db.event.insert(event)
        print 'Done'
   
    #Load keywords from events.csv then write to eventKeywords.txt, for clustering purpose
    #Unfortunately, eventKeywords.txt is still too big for numpy to read(3M events, 630MB size text file)
    def writeEventKeywordsToTxt(self, CSVFileDir):
        lines = self.readCSVFile(CSVFileDir)
        events = []
        for line in lines:
            items = line.strip().split(',')
            events.append([int(item) for item in items[9:]])
        with open('eventKeywords.txt', 'w') as f:
            for event in events:
                try:
                    f.write(' '.join(str(item) for item in event) + '\n')
                except Exception as e:
                    print e
                    continue
    
if __name__ == '__main__':
    dl = DataFileLoader()
    dl.insertUsersToDB('data/users.csv', 'data/user_friends.csv')
    dl.insertEventsToDB('data/events_valid.csv', 'data/event_attendees.csv')
