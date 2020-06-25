#!/usr/bin/python
# -*- coding: latin-1 -*-
# This script is made starting from
#    https://github.com/telefonicaid/fiware-orion/blob/master/scripts/accumulator-server.py
#
# Copyright 2013 Telefonica Investigacion y Desarrollo, S.A.U
#
#

__author__ = 'tmontanaro'

# This script receives all messages from a FIWARE Orion Context Broker and save it to a Postgresql database.
# It is an adaptation of the simple accumulator provided by FIWARE in:
# https://github.com/telefonicaid/fiware-orion/blob/master/scripts/accumulator-server.py
#
# To do its job the script:
# * listens for new data arriving from Orion
# * elaborates incoming data and save it into a database

from OpenSSL import SSL
from flask import Flask, request, Response
from time import sleep
import time
import sys
import os
import atexit
import signal
import json
import logging
from dateutil.parser import parse
import shortuuid
from threading import Lock
import threading
import collections
import databaseInteractions
from datetime import datetime
import multiprocessing
import multiprocessing_logging


# Default arguments then taken from a configuration file
port       = 1028
host       = '192.168.1.100' #it is the IP of the server/pc on which the script is executed
pathForGettingOrionData = '/accumulate'
verbose    = 0
pretty     = False
https      = False
key_file   = None
cert_file  = None
# --- constant connection values
logging.basicConfig(level=logging.DEBUG)
multiprocessing_logging.install_mp_handler()

logger = logging.getLogger(__name__)
lock = Lock()
dbHost = "localhost" #url used by postgresql
dbName = "orion-data-db"
dbUser = "abcuser"
dbPassword = "pass"
threads = []
pidfile = ""

processes = []


# This function is registered to be called upon termination
def all_done():
    os.unlink(pidfile)
    #if any threads is not yet finished, we wait until its end
    for thread in threads:
        if thread.isAlive():
            thread.join(30)
    #if any process is not yet finished, we wait until its end
    for process in processes:
        process.join()

def getCurrentTimeStamp():
    """returns a formatted current time/date"""
    return str(time.strftime("%a %d %b %Y %I:%M:%S %p"))


app = Flask(__name__)
log = logging.getLogger('werkzeug')
log.disabled = True
app.logger.disabled = True

def saveDataInDb(dict, dbHost, dbName, dbUser, dbPassword):
    global logger
    try:
        databaseInteractionsInst = databaseInteractions.DatabaseInteractions(logger, dbHost, dbName, dbUser, dbPassword)
        if (databaseInteractionsInst is not None):
            databaseInteractionsInst.saveDataInDb(dict)

    except Exception as inst:
        now = datetime.now()
        logger.error(str(now) + " - Generic exception in saveDataInDb")
        logger.error(str(inst))  # __str__ allows args to be printed directly

def processRequest(request, dbHost, dbName, dbUser, dbPassword):
    """
    Common function used by serveral route methods to process request content

    :param request: the request to save
    """
    global logger
    s = ''
    try:
        if ((request.data is not None) and (len(request.data) != 0)):
            requestData = json.loads(request.data.decode('utf-8'))
            if (("subscriptionId" in requestData) and (requestData["subscriptionId"] is not None)):
                logger.info("Processing subscription: "+requestData["subscriptionId"])
            if (("data" in requestData) and (requestData["data"] is not None)):
                index = 0
                for singleData in requestData["data"]:
                    dictToSave = {}
                    dictToSave.clear()
                    entityType = ""
                    entityId = ""
                    name = ""
                    farmId = ""
                    if (("type" in singleData) and (singleData["type"] is not None)):
                        entityType = singleData["type"]
                    dictToSave["NGSIentityType"] = entityType
                    for singleKey in singleData.keys():
                        element = singleData[singleKey]
                        value = ""
                        type = ""
                        if (("value" in element) and (element["value"] is not None)):
                            if (singleKey=='additionalInfo'):
                                value = json.dumps(element["value"], indent=4, sort_keys=True)
                                try:
                                    additionalInfo = element["value"]
                                    if ("DEBUG-filename" in additionalInfo):
                                        filename = additionalInfo["DEBUG-filename"]
                                    else:
                                        now = datetime.now()
                                        logger.error(str(now) + "Not possible to get filename - processed additionalInfo: "+value)
                                except Exception as inst:
                                    logger.error("Exception: it was not possible to extract the filename from additionalInfo")
                                    logger.error(str(inst))
                            else:
                                value = element["value"]
                        else:
                            value = element
                        if ((singleKey=="name") and (value is not None)):
                            name = value
                        if ((singleKey=="farmId") and (value is not None)):
                            farmId = value
                        #type to distinguish datetime from string and other stuff
                        if (("type" in element) and (element["type"] is not None)):
                            type = element["type"]

                        if (singleKey.lower() == (entityType.lower()+"id")):
                            entityId = value
                        if (type.lower() == "datetime"):
                            #"value":"2020-03-23T00:00:00.00+0000"
                            try:
                                dt = parse(value)
                                dictToSave[singleKey] = dt.timestamp()
                            except Exception as e:
                                logger.error("It was not possible to transform in timestamp the DateTime contained in "+singleKey)
                                logger.error(str(e))
                                logger.error(e.message)
                        else:
                            dictToSave[singleKey] = value
                    if (entityId!=""):
                        #u = getIdFromEntityIdLocal(entityId)
                        #dictToSave["NGSIentityId"] = u
                        dictToSave["NGSIentityId"] = entityId
                    else:
                        dictToSave["NGSIentityId"] = shortuuid.uuid()
                        logger.error("random uuid generated because the entityId was empty")
                        logger.error(dictToSave)
                    #anyway we send the original uuid4 in a further field
                    dictToSave["OrionEntityId"] = entityId
                    index = index+1
                    dictToSave["originalEntityId"] = dictToSave["NGSIentityId"]
                    dictToSave["originalEntityType"] = dictToSave["NGSIentityType"]

                    #save data in background
                    processes.append(processesPool.apply_async(saveDataInDb, args=(dictToSave, dbHost, dbName, dbUser, dbPassword)))



    except Exception as inst:
        now = datetime.now()
        logger.error(str(now) + " - Generic exception in processRequest")
        logger.error(str(inst))  # __str__ allows args to be printed directly

def sendContinue(request):
    """
    Inspect request header in order to look if we have to continue or not

    :param request: the request to look
    :return: true if we  have to continue, false otherwise
    """

    for h in request.headers.keys():
        if ((h == 'Expect') and (request.headers[h] == '100-continue')):
            sendContinue = True

    return False

@app.route("/v1/updateContext", methods=['POST'])
@app.route("/v1/queryContext", methods=['POST'])
@app.route(pathForGettingOrionData, methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH'])
def acceptUpdates():
    global dbHost, dbName, dbUser, dbPassword
    # Process request
    processRequest(request, dbHost, dbName, dbUser, dbPassword)

    #answer to the Orion Context Broker to do not let it crash
    if sendContinue(request):
        return Response(status=100)
    else:
        return Response(status=200)

def getVarFromConfig():
    global port, host, pathForGettingOrionData, verbose, pretty, https, key_file, cert_file
    global dbName, dbUser, dbHost, dbPassword
    # read configuration parameters from the config.json file
    with open('config.json') as json_data_file:
        data = json.load(json_data_file)
    if 'configuration' in data:
        conf = data['configuration']
        if 'port' in conf:
            port = conf['port']
        if 'host' in conf:
            host = conf['host']
        if 'pathForGettingOrionData' in conf:
            pathForGettingOrionData = conf['pathForGettingOrionData']
        if 'verbose' in conf:
            verbose = conf['verbose']
        if 'pretty' in conf:
            pretty = conf['pretty']
        if 'https' in conf:
            https = conf['https']
        if 'key_file' in conf:
            key_file = conf['key_file']
        if 'cert_file' in conf:
            cert_file = conf['cert_file']
        if 'dbName' in conf:
            dbName = conf['dbName']
        if 'dbUser' in conf:
            dbUser = conf['dbUser']
        if 'dbHost' in conf:
            dbHost = conf['dbHost']
        if 'dbPassword' in conf:
            dbPassword = conf['dbPassword']
        if 'loggingLevel' in conf:
            loggingLevel = conf['loggingLevel']
            setLogging = False
            if (loggingLevel == 'DEBUG'):
                logger.setLevel(logging.DEBUG)
                handler.setLevel(logging.DEBUG)
                setLogging = True
            if (loggingLevel == 'BASIC_FORMAT'):
                logger.setLevel(logging.BASIC_FORMAT)
                handler.setLevel(logging.BASIC_FORMAT)
                setLogging = True
            if (loggingLevel == 'CRITICAL'):
                logger.setLevel(logging.CRITICAL)
                handler.setLevel(logging.CRITICAL)
                setLogging = True
            if (loggingLevel == 'ERROR'):
                logger.setLevel(logging.ERROR)
                handler.setLevel(logging.ERROR)
                setLogging = True
            if (loggingLevel == 'FATAL'):
                logger.setLevel(logging.FATAL)
                handler.setLevel(logging.FATAL)
                setLogging = True
            if (loggingLevel == 'INFO'):
                logger.setLevel(logging.INFO)
                handler.setLevel(logging.INFO)
                setLogging = True
            if (loggingLevel == 'WARN'):
                logger.setLevel(logging.WARN)
                handler.setLevel(logging.WARN)
                setLogging = True
            if (loggingLevel == 'WARNING'):
                logger.setLevel(logging.WARNING)
                handler.setLevel(logging.WARNING)
                setLogging = True
            if (loggingLevel == 'NOTSET'):
                logger.setLevel(logging.NOTSET)
                handler.setLevel(logging.NOTSET)
                setLogging = True
            if (not setLogging):
                logger.setLevel(logging.INFO)
                handler.setLevel(logging.INFO)

def initialization(nameForLogger):
    global logging, logger, handler, formatter
    global https, key_file, cert_file, verbose, port, host, pathForGettingOrionData, pretty, pid
    try:
        logger = logging.getLogger(nameForLogger)
        logger.setLevel(logging.DEBUG)

        # create a file handler
        handler = logging.FileHandler('log.log')
        handler.setLevel(logging.DEBUG)

        # create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # add the handlers to the logger
        logger.addHandler(handler)

        logger.info("loading configuration set")

        getVarFromConfig()
        if https:
            if key_file is None or cert_file is None:
                logger.error("if --https is used then you have to provide --key and --cert")
                sys.exit(1)
        if verbose:
            logger.info("verbose mode is on")
            logger.info("port: " + str(port))
            logger.info("host: " + str(host))
            logger.info("pathForGettingOrionData: " + str(pathForGettingOrionData))
            logger.info("pretty: " + str(pretty))
            logger.info("https: " + str(https))
            if https:
                logger.info("key file: " + key_file)
                logger.info("cert file: " + cert_file)

        # check if the port is already used by anyone else
        pid = str(os.getpid())
        pidfile = "/tmp/accumulator." + str(port) + ".pid"

#
        # If an accumulator process is already running, it is killed.
        # First using SIGTERM, then SIGINT and finally SIGKILL
        # The exception handling is needed as this process dies in case
        # a kill is issued on a non-running process ...
        #
        if os.path.isfile(pidfile):
            file = open(pidfile, 'r')
            oldpid = file.read()
            file.close()
            opid = int(oldpid)
            logger.error("PID file %s already exists, killing the process %s" % (pidfile, oldpid))

            try:
                oldstderr = sys.stderr
                sys.stderr = open("/dev/null", "w")
                os.kill(opid, signal.SIGTERM);
                sleep(0.1)
                os.kill(opid, signal.SIGINT);
                sleep(0.1)
                os.kill(opid, signal.SIGKILL);
                sys.stderr = oldstderr
            except:
                now = datetime.now()
                logger.error(str(now) + " - Process %d killed" % opid)


        #
        # Creating the pidfile of the currently running process
        #
        with open(pidfile, 'w') as file:
            file.write(pid)

        #
        # Making the function all_done being executed on exit of this process.
        # all_done removes the pidfile
        #
        atexit.register(all_done)

        # Note that using debug=True breaks the the procedure to write the PID into a file. In particular
        # makes the calle os.path.isfile(pidfile) return True, even if the file doesn't exist. Thus,
        # use debug=True below with care :)
        if (https):
            # According to http://stackoverflow.com/questions/28579142/attributeerror-context-object-has-no-attribute-wrap-socket/28590266, the
            # original way of using context is deprecated. New way is simpler. However, we are still testing this... some environments
            # fail in some configurations (the current one is an attempt to make this to work at jenkins)
            context = SSL.Context(SSL.SSLv23_METHOD)
            context.use_privatekey_file(key_file)
            context.use_certificate_file(cert_file)
            # context = (cert_file, key_file)
            app.run(host=host, port=port, debug=False, ssl_context=context)
        else:
            app.run(host=host, port=port)
    except Exception as inst:
        now = datetime.now()
        logger.error(str(now) + " - Main Error - " + getCurrentTimeStamp())
        logger.error(str(inst))

#we have to instantiate it here (after the definition of all the methods) otherwise it does not work
processesPool = multiprocessing.Pool(multiprocessing.cpu_count())

if __name__ == '__main__':
        initialization(__name__)

