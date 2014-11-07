import synapseclient
import os
import inspect
import sys
import fileReader
import numpy as np

# syn=synapseclient.Synapse()
# syn.login()
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def fixAnnotationRow(row, delimiter=","):
    """Merge column values in Synapse annotations.
    
    """
    
    for k, v in row.iteritems():
        if type(v) == list:
            row[k] = delimiter.join(v)
            
    return row

def fixRow(row):
    return fixAnnotationRow(row)

def thisCodeInSynapse(parentId, syn, file=None, description=''):
    """Determines the name of the file that the code is called from
    and uploads that to Synapse returning the synapseId of created codeObject.
    """
    #print inspect.getfile(inspect.currentframe())
    #print os.path.abspath(inspect.getfile(inspect.currentframe()))
    file = inspect.getfile(sys._getframe(1)) if file==None else file
    #Make sure unallowed characters are striped out for the name
    code= synapseclient.File(file, name=os.path.split(file)[-1], parent=parentId, description=description)
    codeEntity = syn.store(code)
    return codeEntity

_syn_obj_types = ['entity', 'file', 'link', 'directory']

_syn_obj_keys = ['benefactorId', 'nodeType', 'concreteType',
                'createdByPrincipalId', 'createdOn',
                'createdByPrincipalId', 'eTag', 'id',
                'modifiedOn', 'modifiedByPrincipalId',
                'noteType', 'versionLabel', 'versionComment',
                'versionNumber', 'parentId', 'description']

_entity_keys = ["%s.%s" % (x, y) for x in _syn_obj_types for y in _syn_obj_keys]

def query2df(queryContent, filterSynapseFields=True, delimiter=None):
    """Converts the returned query object from Synapse into a Pandas DataFrame

    Arguments:
    - `queryContent`: content returned from query
    - `filterSynapseFields`: Removes Synapse properties of the entity returned with "select * ..."
    - `delimiter`: if None, takes the first item of list-type content; otherwise joins the data based on delimiter
    
    """

    import pandas as pd

    if type(queryContent) is dict:
        queryContent = queryContent['results']

    
    queryContentDf = pd.DataFrame(queryContent)
    
    #Remove the unecessary lists and 'entity' in names (this should be fixed on Synapse!)
    for key in queryContentDf.keys():
        
        if filterSynapseFields and key in _entity_keys:
            del queryContentDf[key]
            continue

        newkey = key

        # Clean up the column names
        for keytype in _syn_obj_types:
            # logger.debug("Keytype: %s, key: %s" % (keytype, key))
            if key.startswith(keytype):
                newkey = key.replace("%s." % keytype, "")
                break
                
        queryContentDf[newkey] = queryContentDf[key]

        # Clean up the data
        newseries = []
        
        # Iterate over cells of a column
        for cell in queryContentDf[key]:
            if type(cell) is list:
                if delimiter:
                    newseries.append(delimiter.join(cell))
                else:
                        newseries.append(cell[0])
            else:
                newseries.append(cell)

        queryContentDf[newkey] = pd.Series(newseries)
        
        del queryContentDf[key]
        
    return queryContentDf



def createEvaluationBoard(name, syn, parentId, markdown='',status='OPEN', contentSource=''):
        """Creates an evaluation where users can submit entities for evaluation by a monitoring process.
        Currently also creates a folder where the results are displayed in a leaderboard.
        
        Arguments:
        - `name`: Name of the Evaluation
        - `parentId` : Location of leaderboard folder
        - `description`: A string describing the evaluation in detail
        - `status`: A string describing the status: one of { PLANNED, OPEN, CLOSED, COMPLETED}

        Returns:
        - `evaluation`: information about the evaluation 
        - `leaderboard`: folder that contains the output leaderboard
        """
        # # Create an evaluation
        evaluation=syn.store(Evaluation(name=name, status=status, contentSource=contentSource))
        
        # # create a wiki to describe the Challenge
        homeWikiPage = Wiki(title=name, markdown=md, owner=Evaluation)
        homeWikiPage = syn.store(homeWikiPage)

        return evaluation, homeWikiPage
        

def updateLeaderboard(syn, leaderboard, evaluation):
    """Goes through all submissions in an evalution and updates the description markdown in entity.
    
    Arguments:
    - `entity`: A folder/project entity that contains the leaderboard
    - `evaluation`: an evaluation object where submissions are made
    """
    leaderboard.markdown = 'Submission Name|Submitter|Submission|Score|Status|Report|\n'
    userNames={}
    for submission in  syn.getSubmissions(evaluation):
        status =  syn.getSubmissionStatus(submission)
        #Extract the username
        userName = userNames.get(submission['userId'], None)
        if userName==None:
            userName = syn.getUserProfile(submission['userId'])['displayName']
            userNames[submission['userId']] = userName
        print submission.get('name', ''), userName, submission.entityId, status.score, status.status, status.report
        leaderboard.markdown+='%s|%s|%s|%f|%s|%s|\n' %(submission.get('name', ''), 
                                                 userName, submission.entityId, status.score,
                                                 status.status, status.report)
    syn.store(leaderboard)



def readEntityFile(entity):
    return fileReader.csv2Dict(os.path.join(entity['cacheDir'], entity['files'][0]))


def match(seq1, seq2):
    """Finds the index locations of seq1 in seq2"""
    return [ np.nonzero(seq2==x)[0][0] for x in seq1  if x in seq2 ]
        

if __name__ == '__main__':
    thisCodeInSynapse('syn537704')
    print 'done'
