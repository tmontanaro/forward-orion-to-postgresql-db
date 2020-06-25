BEGIN TRANSACTION;
INSERT INTO measure(NGSIentityId, NGSIentityType, buildingId,
            farmId, penId, companyId, compartmentId, parentCompartmentId, name,
            address, ownerCompany, pigId, OrionEntityId, lastUpdate, humidity,
            temperature, luminosity, waterFlow, foodFlow, CO2, numAnimals, avgWeight,
            weightStDev, avgGrowth, additionalInfo, sex, deadAnimalsSinceDateOfArrival, arrivalTimestamp,
            feedConsumption, outputFeed, waterConsumption, startWeight, weight, totalConsumedWater, totalConsumedFood,
            totalTimeConsumedWater, totalTimeConsumedFood, serialNumber, startTimestampAcquisition,
            endTimestampAcquisition, startTimestampMonitoring, endTimestampMonitoring)
                          VALUES ("","","","","","","","","","","","","",123456789,0,0,0,0,0,0,0,0,0,0,"","",0,0,0,0,0,0,0,0,0,0,0,"",0,0,0,0) ON CONFLICT DO NOTHING;
COMMIT;
