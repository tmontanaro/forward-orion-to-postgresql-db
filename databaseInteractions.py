#!/usr/bin/python

__author__ = 'tmontanaro'

# This script is responsible for all the interactions with a postgresql database
# it implements a singleton class. To use it you can use the following instructions:
#   logging.basicConfig(level=logging.DEBUG
#   multiprocessing_logging.install_mp_handler()
#   logger = logging.getLogger(__name__)
#   logger.setLevel(logging.DEBUG)
#   databaseInteractionsInst = databaseInteractions.DatabaseInteractions(logger, dbHost, dbName, dbUser, dbPassword)

from sqlite3 import Error
import threading
from singleton_decorator import singleton
import psycopg2
from datetime import datetime

@singleton
class DatabaseInteractions(object):
    logger = None
    lock = threading.Lock()

    dbHost = "localhost"  # url used by postgresql
    dbName = "orion-data-db"
    dbUser = "abcuser"
    dbPassword = "pass"
    threads = []

    __instance = None

    def __init__(self, extLogger, dbHost, dbName, dbUser, dbPassword):
        self.dbHost = dbHost
        self.dbName = dbName
        self.dbUser = dbUser
        self.dbPassword = dbPassword
        self.logger = extLogger
        self.initializeDb()
        DatabaseInteractions.__instance = self

    def insertDataAtStartup(self):
        try:
            connect_str = "dbname='%s' user='%s' host='%s' password='%s'" % (
            self.dbName, self.dbUser, self.dbHost, self.dbPassword)
            # use our connection values to establish a connection
            conn = psycopg2.connect(connect_str)
            # create a psycopg2 cursor that can execute queries
            cursor = conn.cursor()
            cursor.execute(open("initializeDb.sql", "r").read())
            conn.commit()  # <--- makes sure the change is shown in the database
            cursor.close()
            conn.close()
        except Error as e:
            now = datetime.now()
            self.logger.error(str(now) + " - Generic error in insertDataAtStartup")
            self.logger.error(e)

    def executeCommandOnTable(self, sql_query, params=None):
        result = {}
        try:
            self.lock.acquire(True)
            connect_str = "dbname='%s' user='%s' host='%s' password='%s'"% (self.dbName, self.dbUser, self.dbHost, self.dbPassword)
            # use our connection values to establish a connection
            conn = psycopg2.connect(connect_str)
            # create a psycopg2 cursor that can execute queries
            cursor = conn.cursor()
            cursor.execute(sql_query, params)
            conn.commit()  # <--- makes sure the change is shown in the database
            if sql_query.lower().strip().startswith("select"):
                rows = cursor.fetchall()
                columns = list(map(lambda x: x[0], cursor.description))
                result = (rows, columns)
            cursor.close()
            conn.close()
        except Error as e:
            now = datetime.now()
            self.logger.error(str(now) + " - Generic error in executeCommandOnTable")
            self.logger.error(e)
        finally:
            self.lock.release()
        return result

    def saveDataInDb(self, dictToSave):
        """
            Save data in the measure table
            :param dictToSave:
            """

        NGSIentityId = ""
        NGSIentityType = ""
        buildingId = ""
        farmId = ""
        penId = ""
        companyId = ""
        compartmentId = ""
        parentCompartmentId = ""
        name = ""
        address = ""
        ownerCompany = ""
        pigId = ""
        OrionEntityId = ""
        lastUpdate = -1
        humidity = ""
        temperature = ""
        luminosity = ""
        waterFlow = ""
        foodFlow = ""
        CO2 = ""
        numAnimals = ""
        avgWeight = ""
        weightStDev = ""
        avgGrowth = ""
        additionalInfo = ""
        sex = ""
        deadAnimalsSinceDateOfArrival = ""
        arrivalTimestamp = ""
        feedConsumption = ""
        outputFeed = ""
        waterConsumption = ""
        startWeight = ""
        weight = ""
        totalConsumedWater = ""
        totalConsumedFood = ""
        totalTimeConsumedWater = ""
        totalTimeConsumedFood = ""
        serialNumber = ""
        startTimestampAcquisition = ""
        endTimestampAcquisition = ""
        startTimestampMonitoring = ""
        endTimestampMonitoring = ""

        try:
            if ("NGSIentityId" in dictToSave):
                NGSIentityId = dictToSave["NGSIentityId"]
            if ("NGSIentityType" in dictToSave):
                NGSIentityType = dictToSave["NGSIentityType"]
            if ("buildingId" in dictToSave):
                buildingId = dictToSave["buildingId"]
            if ("farmId" in dictToSave):
                farmId = dictToSave["farmId"]
            if ("penId" in dictToSave):
                penId = dictToSave["penId"]
            if ("companyId" in dictToSave):
                companyId = dictToSave["companyId"]
            if ("compartmentId" in dictToSave):
                compartmentId = dictToSave["compartmentId"]
            if ("parentCompartmentId" in dictToSave):
                parentCompartmentId = dictToSave["parentCompartmentId"]
            if ("name" in dictToSave):
                name = dictToSave["name"]
            if ("address" in dictToSave):
                address = dictToSave["address"]
            if ("ownerCompany" in dictToSave):
                ownerCompany = dictToSave["ownerCompany"]
            if ("pigId" in dictToSave):
                pigId = dictToSave["pigId"]
            if ("OrionEntityId" in dictToSave):
                OrionEntityId = dictToSave["OrionEntityId"]
            if ("lastUpdate" in dictToSave):
                lastUpdate = dictToSave["lastUpdate"]
            if ("humidity" in dictToSave):
                humidity = dictToSave["humidity"]
            if ("temperature" in dictToSave):
                temperature = dictToSave["temperature"]
            if ("luminosity" in dictToSave):
                luminosity = dictToSave["luminosity"]
            if ("waterFlow" in dictToSave):
                waterFlow = dictToSave["waterFlow"]
            if ("foodFlow" in dictToSave):
                foodFlow = dictToSave["foodFlow"]
            if ("CO2" in dictToSave):
                CO2 = dictToSave["CO2"]
            if ("numAnimals" in dictToSave):
                numAnimals = dictToSave["numAnimals"]
            if ("avgWeight" in dictToSave):
                avgWeight = dictToSave["avgWeight"]
            if ("weightStDev" in dictToSave):
                weightStDev = dictToSave["weightStDev"]
            if ("avgGrowth" in dictToSave):
                avgGrowth = dictToSave["avgGrowth"]
            if ("additionalInfo" in dictToSave):
                additionalInfo = dictToSave["additionalInfo"]
            if ("sex" in dictToSave):
                sex = dictToSave["sex"]
            if ("deadAnimalsSinceDateOfArrival" in dictToSave):
                deadAnimalsSinceDateOfArrival = dictToSave["deadAnimalsSinceDateOfArrival"]
            if ("arrivalTimestamp" in dictToSave):
                arrivalTimestamp = dictToSave["arrivalTimestamp"]
            if ("feedConsumption" in dictToSave):
                feedConsumption = dictToSave["feedConsumption"]
            if ("outputFeed" in dictToSave):
                outputFeed = dictToSave["outputFeed"]
            if ("waterConsumption" in dictToSave):
                waterConsumption = dictToSave["waterConsumption"]
            if ("startWeight" in dictToSave):
                startWeight = dictToSave["startWeight"]
            if ("weight" in dictToSave):
                weight = dictToSave["weight"]
            if ("totalConsumedWater" in dictToSave):
                totalConsumedWater = dictToSave["totalConsumedWater"]
            if ("totalConsumedFood" in dictToSave):
                totalConsumedFood = dictToSave["totalConsumedFood"]
            if ("totalTimeConsumedWater" in dictToSave):
                totalTimeConsumedWater = dictToSave["totalTimeConsumedWater"]
            if ("totalTimeConsumedFood" in dictToSave):
                totalTimeConsumedFood = dictToSave["totalTimeConsumedFood"]
            if ("serialNumber" in dictToSave):
                serialNumber = dictToSave["serialNumber"]
            if ("startTimestampAcquisition" in dictToSave):
                startTimestampAcquisition = dictToSave["startTimestampAcquisition"]
            if ("endTimestampAcquisition" in dictToSave):
                endTimestampAcquisition = dictToSave["endTimestampAcquisition"]
            if ("startTimestampMonitoring" in dictToSave):
                startTimestampMonitoring = dictToSave["startTimestampMonitoring"]
            if ("endTimestampMonitoring" in dictToSave):
                endTimestampMonitoring = dictToSave["endTimestampMonitoring"]

            sql = ''' INSERT INTO measure(NGSIentityId, NGSIentityType, buildingId,
            farmId, penId, companyId, compartmentId, parentCompartmentId, name,
            address, ownerCompany, pigId, OrionEntityId, lastUpdate, humidity,
            temperature, luminosity, waterFlow, foodFlow, CO2, numAnimals, avgWeight,
            weightStDev, avgGrowth, additionalInfo, sex, deadAnimalsSinceDateOfArrival, arrivalTimestamp,
            feedConsumption, outputFeed, waterConsumption, startWeight, weight, totalConsumedWater, totalConsumedFood,
            totalTimeConsumedWater, totalTimeConsumedFood, serialNumber, startTimestampAcquisition,
            endTimestampAcquisition, startTimestampMonitoring, endTimestampMonitoring)
                          VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '''

            # open the connection with the database
            params = (NGSIentityId, NGSIentityType, buildingId,
                      farmId, penId, companyId, compartmentId, parentCompartmentId, name,
                      address, ownerCompany, pigId, OrionEntityId, lastUpdate, humidity,
                      temperature, luminosity, waterFlow, foodFlow, CO2, numAnimals, avgWeight,
                      weightStDev, avgGrowth, additionalInfo, sex, deadAnimalsSinceDateOfArrival, arrivalTimestamp,
                      feedConsumption, outputFeed, waterConsumption, startWeight, weight, totalConsumedWater,
                      totalConsumedFood,
                      totalTimeConsumedWater, totalTimeConsumedFood, serialNumber, startTimestampAcquisition,
                      endTimestampAcquisition, startTimestampMonitoring, endTimestampMonitoring)
            #lastRowId = None
        except Exception as e:
            now = datetime.now()
            self.logger.error(str(now) + " - Problem in saveDataInDb")
            self.logger.error(str(e))
        self.executeCommandOnTable(sql, params)

    def initializeDb(self, ):

        sqlCreateMeasureTable = """ CREATE TABLE IF NOT EXISTS measure (
                                    measureId SERIAL PRIMARY KEY,
                                    sqltime TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                                    NGSIentityId TEXT,
                                    NGSIentityType TEXT,
                                    buildingId TEXT,
                                    farmId TEXT,
                                    penId TEXT,
                                    companyId TEXT,
                                    compartmentId TEXT,
                                    parentCompartmentId TEXT,
                                    name TEXT,
                                    address TEXT,
                                    ownerCompany TEXT,
                                    pigId TEXT,
                                    OrionEntityId TEXT,
                                    lastUpdate REAL,
                                    humidity TEXT,
                                    temperature TEXT,
                                    luminosity TEXT,
                                    waterFlow TEXT,
                                    foodFlow TEXT,
                                    CO2 TEXT,
                                    numAnimals TEXT,
                                    avgWeight TEXT,
                                    weightStDev TEXT,
                                    avgGrowth TEXT,
                                    additionalInfo TEXT,
                                    sex TEXT,
                                    deadAnimalsSinceDateOfArrival TEXT,
                                    arrivalTimestamp TEXT,
                                    feedConsumption TEXT,
                                    outputFeed TEXT,
                                    waterConsumption TEXT,
                                    startWeight TEXT,
                                    weight TEXT,
                                    totalConsumedWater TEXT,
                                    totalConsumedFood TEXT,
                                    totalTimeConsumedWater TEXT,
                                    totalTimeConsumedFood TEXT,
                                    serialNumber TEXT,
                                    startTimestampAcquisition TEXT,
                                    endTimestampAcquisition TEXT,
                                    startTimestampMonitoring TEXT,
                                    endTimestampMonitoring TEXT
                                );
                                    """
        #delete data that are older than 2 days
        sqlCreateMeasureTrigger = """ DROP TRIGGER IF EXISTS delete_tail
                                    ON measure;
                                    CREATE OR REPLACE FUNCTION dropOldData() RETURNS TRIGGER AS $$
                                       BEGIN
                                          DELETE FROM measure WHERE sqltime < now()-'2 day'::interval;
                                          RETURN NULL;
                                       END;
                                       $$
                                       LANGUAGE 'plpgsql';
                                    CREATE TRIGGER delete_tail
                                    AFTER INSERT
                                    ON measure
                                    FOR EACH ROW EXECUTE PROCEDURE dropOldData();
                                    """
        # create tables

        # create measure table
        self.executeCommandOnTable(sqlCreateMeasureTable)
        # create trigger
        self.executeCommandOnTable(sqlCreateMeasureTrigger)

        #import default data
        #self.insertDataAtStartup()