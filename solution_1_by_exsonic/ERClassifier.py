'''
Created on 2013-4-4
@author: Bobi Pu, bobi.pu@usc.edu
'''
import numpy
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics.metrics import accuracy_score, classification_report

class ERClassifier(object):
    def __init__(self):
        self.classifier = None
    
    def loadData(self, dataFileDir):
        data = numpy.loadtxt(dataFileDir)
        X = data[:, 0:-1]
        y = data[:, -1]
        return X, y
            
    def trainModel(self, X, y):
        classifiers = [SVC(kernel='rbf'), RandomForestClassifier(), DecisionTreeClassifier()]
        self.classifier = classifiers[2]
        self.classifier.n_classes_ = 2
        self.classifier.fit(X, y)
        
    def crossValidation(self, X, y, k=10):
        pass
    
    def predict(self, X):
        return self.classifier.predict(X)
        
    def Run(self, trainFileDir, testFileDir):
        XTrain, yTrain = self.loadData(trainFileDir)
        self.trainModel(XTrain, yTrain)
        XTest, yTest = self.loadData(testFileDir)
        yPred = self.predict(XTest)
        accuracy = accuracy_score(yTest, yPred)
        #precision, recall, fScore, _ = precision_recall_fscore_support(y, yPred) 
        labels = [1, 0]
        classNames = ['interested', 'notInterested']
        report = classification_report(yTest, yPred, labels=labels, target_names=classNames) + '\naccuracy\t' + str(accuracy)
        print report
        
if __name__ == '__main__':
    clf = ERClassifier()
    clf.Run('feature.txt', 'test.txt')
    