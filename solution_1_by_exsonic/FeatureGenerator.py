'''
Created on 2013-4-5
@author: Bobi Pu, bobi.pu@usc.edu
'''
import sys, numpy
from pymongo import MongoClient
from DataFileLoader import DataFileLoader
from sklearn.cluster import KMeans
from geopy import geocoders, distance
from dateutil import parser

class FeatureGenerator(object):
    def __init__(self):
        self.attendeeType = ['willAttendUsers', 'notAttendUsers', 'mayAttendUsers', 'invitedUsers']
        self.clusterer = None
        try:
            self.db = MongoClient().EventRecommender
        except Exception as e:
            print e
            sys.exit()
    
    def loadData(self, CSVFileDir):
        #read to dict, key is userId_eventId
        lines = DataFileLoader().readCSVFile(CSVFileDir)
        trainData = []
        for line in lines:
            try:
                items = line.strip().split(',')
                train = {}
                train['userId'] = items[0]
                train['eventId'] = items[1]
                train['invited'] = int(items[2])
                train['firstSawTime'] = parser.parse(items[3]).replace(tzinfo=None) #remove the timezone-awareness
                train['interested'] = int(items[4])
                train['notInterested'] = int(items[5])
                trainData.append(train)
            except Exception as e:
                print e
                continue
        return trainData
    
    def getFeatureMatrix(self, CSVFileDir):
        trainData = self.loadData(CSVFileDir)
        self.loadClusteringModel()
        featureMatrix = []
        for i, train in enumerate(trainData):
            featureVector = self.getFeatureVector(train)
            featureVector.append(self.getClassificationY(train))
            featureMatrix.append(featureVector)
            print i
        return featureMatrix 
    
    def getFeatureVector(self, train):
        featureVector = []
        userId, eventId, firstSawTime, invited = train['userId'], train['eventId'], train['firstSawTime'], train['invited']
        event = self.db.event.find_one({'id' : eventId})
        user = self.db.user.find_one({'id' : userId})
        featureVector.extend(self.getNumOfAttendees(event))
        featureVector.extend(self.getNumOfFriendAttendees(user, event))
        featureVector.extend(self.getNumOfSimilarUsers(user, event, numOfType=1))
        featureVector.extend(self.getEventSimilarity(user, event, numOfType=1))
#         featureVector.extend(self.getEventSimilarity(user, event, isInFriend=True, numOfType=1))
        featureVector.append(self.getLocationDistance(user, event, isSearchEachTime=True))
        featureVector.append(self.getTimeDifference(user, event, firstSawTime))
#         featureVector.append(self.getAgeOrGenderDifference(user, event, 'gender'))
#         featureVector.append(self.getAgeOrGenderDifference(user, event, 'age'))
        featureVector.append(self.isHostFriend(user, event))
        featureVector.append(invited)
        return featureVector
    
    def getClassificationY(self, train):
        if train['interested'] == 1:
            return 1
        else:
            return 0
    
    def isHostFriend(self, user, event):
        return 1 if event['hostUserId'] in user['friends'] else 0
    
    def getNumOfAttendees(self, event):
        numOfWillAttend = len(event[self.attendeeType[0]])
        numOfMayAttend = len(event[self.attendeeType[1]])
        numOfNotAttend = len(event[self.attendeeType[2]])
        numOfInvited = len(event[self.attendeeType[3]])
        return numOfWillAttend, numOfMayAttend, numOfNotAttend, numOfInvited
    
    def getNumOfFriendAttendees(self, user, event):
        numOfWillAttend, numOfMayAttend, numOfNotAttend, numOfInvited = 0, 0, 0, 0
        try:
            friends = user['friends']
            numOfWillAttend = len(set(friends).intersection(event[self.attendeeType[0]]))
            numOfMayAttend = len(set(friends).intersection(event[self.attendeeType[1]]))
            numOfNotAttend = len(set(friends).intersection(event[self.attendeeType[2]]))
            numOfInvited = len(set(friends).intersection(event[self.attendeeType[3]]))
        except Exception as e:
            print e
        return numOfWillAttend, numOfMayAttend, numOfNotAttend, numOfInvited
    
    #compute the distance between user and event, Google API, MUST connect to internet, return in miles
    #if don't have user location info, return 0
    def getLocationDistance(self, user, event, isSearchEachTime=False):
        g = geocoders.GeoNames()
        ueDistance = 0
        if user['location'] is not None and event['latitude'] is not None and event['longitude'] is not None:
            try:
                #if distance more than 300, consider as got an ERROR coordinate  
                ueDistance = 300
                eventCoordinate = (event['latitude'], event['longitude'])
                if 'latitude' in user and isSearchEachTime == False:
                    userCoordinate = (user['latitude'], user['longitude'])
                    d = distance.distance(userCoordinate, eventCoordinate).miles
                    ueDistance = d if d < ueDistance else ueDistance
                else:
                    #if retrieved multiple locations, choose the closest one
                    results = g.geocode(user['location'], exactly_one = False)
                    closestIndex = 0
                    for i, (_, userCoordinate) in enumerate(results):
                        d = distance.distance(userCoordinate, eventCoordinate).miles
                        if d < ueDistance:
                            ueDistance = d
                            closestIndex = i
                    self.db.user.update({'id' : user['id']}, {'$set' : {'latitude' : results[closestIndex][1][0], 
                                                                        'longitude' : results[closestIndex][1][1]}})
            except Exception as e:
                print e
        return ueDistance
    
    def getNumOfSimilarUsers(self, user, event, numOfType = 4):
        #get the number of users who willAttend/mayAttend/notAttend/invited this event and also willAttend/mayAttend/notAttend/invited events that this user did
        #get will attend users for this event
        inputUserId = user['id']
        similarUsersNum = [0] * numOfType
        for i, attendeeType in enumerate(self.attendeeType[0 : numOfType]):
            try:
                for e in self.db.event.find({attendeeType : {'$in' : [inputUserId]}}):
                    for userId in event[attendeeType]:
                        similarUsersNum[i] += (1 if userId in e[attendeeType] else 0)
            except Exception as e:
                print e
                continue
        return similarUsersNum
    
    #compute similarity value between input event and events user(or his friend) "attended" before 
    def getEventSimilarity(self, user, event, isInFriend = False, numOfClusters = 10, numOfType = 4):
        inputUserId = user['id']
        eventSimilarityValues = [0] * numOfType
        predictedCluster = self.clusterer.predict(numpy.array(event['keywords']).astype(float))[0]
        for i, attendeeType in enumerate(self.attendeeType[0 : numOfType]):
            try:
                clusterRecord = [0] * numOfClusters
                #get the events that input user has willAttend/mayAttend/notAttend/invited, then cluster those events
                #compute the value that total num of events devided by how many of them in input events' cluster 
                events = []
                if  isInFriend:
                    friends = user['friends']
                    #so many friens, extremely slow, this feature doesn't help much
                    for userId in friends:
                        events.extend(list(self.db.event.find({attendeeType : {'$in' : [userId]}})))
                else:
                    events = list(self.db.event.find({attendeeType : {'$in' : [inputUserId]}}))
                for e in events:
                    #cluster these events
                    n = self.clusterer.predict(numpy.array(e['keywords']).astype(float))[0]
                    clusterRecord[n] += 1
                eventSimilarityValues[i] = (float(clusterRecord[predictedCluster]) / sum(clusterRecord)) if sum(clusterRecord) != 0 else 0
            except Exception as e:
                print e
                continue
        return eventSimilarityValues
            
    def loadClusteringModel(self, numOfClusters = 10):
        model = self.db.cluster.find_one({'k' : numOfClusters})
        if model is None:
            raise Exception('Load clustering Model error, no such model for ' + numOfClusters + ' clusters')
        centers = numpy.array(model['centers'])
        self.clusterer = KMeans(n_clusters= numOfClusters, n_init = 1, init = centers).fit(centers)
                    
    #limitNum range from 0 to 3 million, 0 means No limit, default 10000
    def trainClusteringModel(self, isDataFromFile = False, dataFileDir = None, limitNum = 10000, numOfClusters = 10):
        if isDataFromFile:
            data = numpy.loadtxt(dataFileDir)
        else:
            try:
                events = self.db.event.find().limit(limitNum)
                eventKeywords = [event['keywords'] for event in events]
            except Exception as e:
                print e
            data = numpy.array(eventKeywords)
        kMeansModel = KMeans(n_clusters = numOfClusters).fit(data)
        centers = kMeansModel.cluster_centers_.tolist()
        #remove first, then insert
        self.db.cluster.remove({'k' : numOfClusters})
        self.db.cluster.insert({'k' : numOfClusters, 'centers' : centers})
        
    def getTimeDifference(self, user, event, firstSawTime):
        #return hour difference between firstSawTime and StartTime
        diff = event['startTime'] - firstSawTime
        diffHours = diff.days * 24 + diff.seconds / 3600
        return diffHours  
    
    #this method work for both 'gender' and 'age', valueType must be one of them
    #NOTE this feature may be not that useful, because we don't have majority user information
    def getAgeOrGenderDifference(self, user, event, valueType, attendeeTypeNum = 0):
        #get the avg age of will attender this event
        if valueType != 'gender' and valueType != 'age':
            raise Exception('invalid valueType, must be either gender or age')
        inputUserId = user['id']
        attendeeType = self.attendeeType[attendeeTypeNum]
        inputEventAvgValue = self.getAvgValue(event, attendeeType, valueType)
        totalAvgValue = 0
        events = list(self.db.event.find({attendeeType : {'$in' : [inputUserId]}}))
        if len(events) == 0:
            return inputEventAvgValue
        else:
            for e in events:
                totalAvgValue += self.getAvgValue(e, attendeeType, valueType)
            diff = inputEventAvgValue - (totalAvgValue / len(events))
            return diff

    def getAvgValue(self, event, attendeeType, valueType):
        if len(event[attendeeType]) == 0:
            return 0
        total = 0
        validUserNum = 0
        for userId in event[attendeeType]:
            user = self.db.user.find_one({'id' : userId})
            value = 0
            #some user info is missing and some user don't provide age info
            if user is not None and user[valueType] is not None:
                validUserNum += 1
                value = user[valueType]
            total += value
        avg = (float(total) / validUserNum) if validUserNum != 0 else 0.0
        return avg
    
    def writeFeatureMatrixToFile(self, featureMatrix):
        with open('feature.txt', 'w') as f:
            for featureVector in featureMatrix:
                f.write(' '.join([str(value) for value in featureVector]) + '\n')
     
#update lat lng into user
if __name__ == '__main__':
    fg = FeatureGenerator()
#     fg.trainClusteringModel(limitNum=0)
    matrix = fg.getFeatureMatrix('train_small.csv')
    fg.writeFeatureMatrixToFile(matrix)