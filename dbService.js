const { Pool } = require("pg");
const format = require("pg-format");
const fs = require("fs");
const dbService = {
  addAllListData: async (allData) => {
    const pool = new Pool({
      user: process.env.DB_USER,
      host: process.env.DB_HOST,
      database: process.env.DB_DATABASE,
      password: process.env.DB_PASSWORD,
      port: process.env.DB_PORT,
      ssl: true,
    });

    const createdAt = new Date();

    //TODO ask for listing agent
    const listingAgentName = "";
    const listsInsertQuery = format(
      "INSERT INTO lists (listingkey, listofficekey, availabilitydate, propertysubtype, documentsavailable, leaseamount, leaseamountfrequency, businesstype, waterbodyname, view, numberofbuildings, numberofunitstotal, lotfeatures, lotsizearea, lotsizedimensions, lotsizeunits, poolfeatures, roadsurfacetype, currentuse, possibleuse, anchorscotenants, waterfrontfeatures, communityfeatures, appliances, otherequipment, securityfeatures, totalactualrent, existingleasetype, associationfee, associationfeefrequency, associationname, associationfeeincludes, originalentrytimestamp, modificationtimestamp, listingid, internetentirelistingdisplayyn, standardstatus, statuschangetimestamp, publicremarks, listprice, inclusions, colistofficekey, colistagentkey, listagentkey, internetaddressdisplayyn, listingurl, originatingsystemname, photoscount, photoschangetimestamp, unparsedaddress, postalcode, subdivisionname, stateorprovince, streetdirprefix, streetdirsuffix, streetname, streetnumber, streetsuffix, unitnumber, country, city, directions, latitude, longitude, cityregion, parkingtotal, yearbuilt, bathroomspartial, bathroomstotalinteger, bedroomstotal, buildingareatotal, buildingareaunits, buildingfeatures, abovegradefinishedarea, abovegradefinishedareaunits, livingarea, livingareaunits, fireplacestotal, fireplaceyn, fireplacefeatures, architecturalstyle, heating, foundationdetails, basement, exteriorfeatures, flooring, parkingfeatures, cooling, propertycondition, roof, constructionmaterials, stories, propertyattachedyn, zoning, zoningdescription, taxannualamount, taxblock, taxlot, taxyear, structuretype, utilities, irrigationsource, watersource, sewer, electric, commoninterest, media, rooms, listingagentname, createdat) VALUES %L RETURNING listingkey",
      allData.map((data) => [
        data.ListingKey,
        data.ListOfficeKey,
        data.AvailabilityDate,
        data.PropertySubType,
        data.DocumentsAvailable,
        data.LeaseAmount,
        data.LeaseAmountFrequency ? 1 : null,
        data.BusinessType,
        data.WaterBodyName,
        data.View,
        data.NumberOfBuildings,
        data.NumberOfUnitsTotal,
        data.LotFeatures,
        data.LotSizeArea,
        data.LotSizeDimensions,
        data.LotSizeUnits,
        data.PoolFeatures,
        data.RoadSurfaceType,
        data.CurrentUse,
        data.PossibleUse,
        data.AnchorsCoTenants,
        data.WaterfrontFeatures,
        data.CommunityFeatures,
        data.Appliances,
        data.OtherEquipment,
        data.SecurityFeatures,
        data.TotalActualRent,
        data.ExistingLeaseType,
        data.AssociationFee,
        data.AssociationFeeFrequency,
        data.AssociationName,
        data.AssociationFeeIncludes,
        data.OriginalEntryTimestamp,
        data.ModificationTimestamp,
        data.ListingId,
        data.InternetEntireListingDisplayYN,
        data.StandardStatus,
        data.StatusChangeTimestamp,
        data.PublicRemarks,
        data.ListPrice,
        data.Inclusions,
        data.CoListOfficeKey,
        data.CoListAgentKey,
        data.ListAgentKey,
        data.InternetAddressDisplayYN,
        data.ListingURL,
        data.OriginatingSystemName,
        data.PhotosCount,
        data.PhotosChangeTimestamp,
        data.UnparsedAddress,
        data.PostalCode,
        data.SubdivisionName,
        data.StateOrProvince,
        data.StreetDirPrefix,
        data.StreetDirSuffix,
        data.StreetName,
        data.StreetNumber,
        data.StreetSuffix,
        data.UnitNumber,
        data.Country,
        data.City,
        data.Directions,
        data.Latitude,
        data.Longitude,
        data.CityRegion,
        data.ParkingTotal,
        data.YearBuilt,
        data.BathroomsPartial,
        data.BathroomsTotalInteger,
        data.BedroomsTotal,
        data.BuildingAreaTotal,
        data.BuildingAreaUnits,
        data.BuildingFeatures,
        data.AboveGradeFinishedArea,
        data.AboveGradeFinishedAreaUnits,
        data.LivingArea,
        data.LivingAreaUnits,
        data.FireplacesTotal,
        data.FireplaceYN,
        data.FireplaceFeatures,
        data.ArchitecturalStyle,
        data.Heating,
        data.FoundationDetails,
        data.Basement,
        data.ExteriorFeatures,
        data.Flooring,
        data.ParkingFeatures,
        data.Cooling,
        data.PropertyCondition,
        data.Roof,
        data.ConstructionMaterials,
        data.Stories,
        data.PropertyAttachedYN,
        data.Zoning,
        data.ZoningDescription,
        data.TaxAnnualAmount,
        data.TaxBlock,
        data.TaxLot,
        data.TaxYear,
        data.StructureType,
        data.Utilities,
        data.IrrigationSource,
        data.WaterSource,
        data.Sewer,
        data.Electric,
        data.CommonInterest,
        data.Media,
        data.Rooms,
        listingAgentName,
        createdAt,
      ])
    );

    await pool.query("delete from lists", (err, res) => {
      if (err)
        console.error("**Error while deleting existing data in lists", err);
      else console.log("Query executed");
    });

    await pool.query(listsInsertQuery, (err, res) => {
      if (err) console.error("**Error while inserting in lists", err);
      else console.log("Query executed");
      pool.end();
    });
  },
  addListingAgentsData: async (agentsData) => {
    const createdAt = new Date();
    const pool = new Pool({
      user: process.env.DB_USER,
      host: process.env.DB_HOST,
      database: process.env.DB_DATABASE,
      password: process.env.DB_PASSWORD,
      port: process.env.DB_PORT,
      ssl: true,
    });
    await pool.query("delete from listingagentids", (err, res) => {
      if (err)
        console.error(
          "**Error while deleting existing data in listingagentids",
          err
        );
      else console.log("Query executed");
    });
    const listingagentidsQuery = format(
      "INSERT INTO listingagentids (listingagentkeys, agentname, createdat) VALUES %L RETURNING listingagentkeys",
      agentsData.map((data) => [
        data.MemberKey,
        data.MemberFirstName + " " + data.MemberLastName,
        createdAt,
      ])
    );
    // Execute the query
    pool.query(listingagentidsQuery, (err, res) => {
      if (err) console.error(err);
      else console.log("Query executed");
    });
  },
  addDestinationsData: async (data) => {
    const pool = new Pool({
      user: process.env.DB_USER,
      host: process.env.DB_HOST,
      database: process.env.DB_DATABASE,
      password: process.env.DB_PASSWORD,
      port: process.env.DB_PORT,
      ssl: true,
    });
    await pool.query("delete from destinations", (err, res) => {
      if (err)
        console.error(
          "**Error while deleting existing data in destinations",
          err
        );
      else console.log("Query executed");
    });
    const destinationsInsertQuery = format(
      "INSERT INTO destinations (destinationid, destinationname, destinationurl, destinationtype, destinationstatus,memberfirstname,memberlastname,memberkey,originalentrytimestamp,modificationtimestamp) VALUES %L RETURNING destinationid",
      data.map((d) => [
        d.DestinationId,
        d.DestinationName,
        d.DestinationUrl,
        d.DestinationType,
        d.DestinationStatus,
        d.MemberFirstName,
        d.MemberLastName,
        d.MemberKey,
        d.OriginalEntryTimestamp,
        d.ModificationTimestamp,
      ])
    );
    pool.query(destinationsInsertQuery, (err, res) => {
      if (err) console.log("WHile inserting destinations", err);
      else console.log("Query executed");
    });
  },

  cerateSeparateDestination: async (tableName) => {
    const pool = new Pool({
      user: process.env.DB_USER,
      host: process.env.DB_HOST,
      database: process.env.DB_DATABASE,
      password: process.env.DB_PASSWORD,
      port: process.env.DB_PORT,
      ssl: true,
    });
    pool.query(
      `CREATE TABLE IF NOT EXISTS ${tableName} (listingkey bigint,listingagentname text,colistingagentname text,destinationid bigint ,listofficekey bigint,availabilitydate bigint,propertysubtype text,documentsavailable json,leaseamount double precision,leaseamountfrequency bigint,businesstype json,waterbodyname bigint,view json,numberofbuildings bigint,numberofunitstotal bigint,lotfeatures json,lotsizearea double precision,lotsizedimensions text,lotsizeunits text,poolfeatures json,roadsurfacetype json,currentuse json,possibleuse json,anchorscotenants bigint,waterfrontfeatures json,communityfeatures json,appliances json,otherequipment json,securityfeatures json,totalactualrent double precision,existingleasetype json,associationfee double precision,associationfeefrequency text,associationname text,associationfeeincludes json,originalentrytimestamp text,modificationtimestamp text,listingid bigint,internetentirelistingdisplayyn boolean,standardstatus text, statuschangetimestamp text, publicremarks text, publicremarks_old text,listprice bigint,listprice_old bigint,inclusions bigint,colistofficekey bigint,colistagentkey bigint,listagentkey bigint, internetaddressdisplayyn boolean,listingurl text,originatingsystemname text,photoscount bigint,photoschangetimestamp text,unparsedaddress text,postalcode text,subdivisionname bigint,stateorprovince text,streetdirprefix bigint,streetdirsuffix text,streetname text,streetnumber bigint,streetsuffix text,unitnumber text, country text, city text,directions bigint,latitude double precision,longitude double precision,cityregion text,parkingtotal bigint,yearbuilt bigint, bathroomspartial bigint,bathroomstotalinteger bigint,bathroomstotalinteger_old bigint, bedroomstotal bigint, bedroomstotal_old bigint, buildingareatotal bigint, buildingareatotal_old bigint,buildingareaunits text,buildingfeatures json,abovegradefinishedarea bigint,abovegradefinishedareaunits bigint,livingarea bigint,livingareaunits text,fireplacestotal bigint,fireplaceyn boolean,fireplacefeatures json,architecturalstyle json,heating json,foundationdetails json,basement json,exteriorfeatures json, flooring json,parkingfeatures json,cooling json,propertycondition json, roof json,constructionmaterials json,stories character varying,propertyattachedyn character varying,zoning character varying,zoningdescription character varying,taxannualamount double precision,taxblock bigint ,taxlot bigint, taxyear bigint, structuretype json,utilities json,irrigationsource json,watersource json,sewer json,electric json,commoninterest text,media json,rooms json,isdeleted text,createdat text,updatedat text)`,
      (err, res) => {
        if (err) console.log(`**Error While creating table ${tableName}`, err);
        else console.log("Query executed");
      }
    );

    // TODO do you want to keep all the deleted rows in the destinations table till date
    // if yes then we will have to apply condition where isDeleted != "deleted"

    const res = await pool.query(
      `select * from ${tableName}`
      // , (err, res) => {
      //   if (err)
      //     console.log(
      //       `**Error While selecting data from table ${tableName}`,
      //       err
      //     );
      //   else console.log("Query executed");
      // }
    );
    return res.rows;
  },
  populateDestinationTable: async (
    tableName,
    allData,
    destinationId,
    agentData
  ) => {
    const pool = new Pool({
      user: process.env.DB_USER,
      host: process.env.DB_HOST,
      database: process.env.DB_DATABASE,
      password: process.env.DB_PASSWORD,
      port: process.env.DB_PORT,
      ssl: true,
    });
    const createdAt = new Date();

    const destinationsSpecificQuery = format(
      `INSERT INTO ${tableName} (listingkey, listingagentname, colistingagentname, destinationid,  listofficekey,  availabilitydate,  propertysubtype,  documentsavailable, leaseamount, leaseamountfrequency, businesstype, waterbodyname, view, numberofbuildings, numberofunitstotal, lotfeatures, lotsizearea, lotsizedimensions, lotsizeunits, poolfeatures, roadsurfacetype, currentuse, possibleuse, anchorscotenants, waterfrontfeatures, communityfeatures, appliances, otherequipment, securityfeatures, totalactualrent, existingleasetype, associationfee, associationfeefrequency, associationname, associationfeeincludes, originalentrytimestamp, modificationtimestamp, listingid, internetentirelistingdisplayyn, standardstatus, statuschangetimestamp, publicremarks, listprice, inclusions, colistofficekey, colistagentkey, listagentkey, internetaddressdisplayyn, listingurl, originatingsystemname, photoscount, photoschangetimestamp, unparsedaddress, postalcode, subdivisionname, stateorprovince, streetdirprefix, streetdirsuffix, streetname, streetnumber, streetsuffix, unitnumber, country, city, directions, latitude, longitude, cityregion, parkingtotal, yearbuilt, bathroomspartial, bathroomstotalinteger, bedroomstotal, buildingareatotal, buildingareaunits, buildingfeatures, abovegradefinishedarea, abovegradefinishedareaunits, livingarea, livingareaunits, fireplacestotal, fireplaceyn, fireplacefeatures, architecturalstyle, heating, foundationdetails, basement, exteriorfeatures, flooring, parkingfeatures, cooling, propertycondition, roof, constructionmaterials, stories, propertyattachedyn, zoning, zoningdescription, taxannualamount, taxblock, taxlot, taxyear, structuretype, utilities, irrigationsource, watersource, sewer, electric, commoninterest, media, rooms, createdat) VALUES %L returning listingkey`,
      allData.map((data) => {
        const f = [
          data.ListingKey ?? null,
          getAgentName(agentData, data.ListAgentKey),
          getAgentName(agentData, data.CoListAgentKey),
          destinationId ?? null,
          data.ListOfficeKey ?? null,
          data.AvailabilityDate ?? null,
          data.PropertySubType ?? null,
          data.DocumentsAvailable ?? null,
          data.LeaseAmount ?? null,
          data.LeaseAmountFrequency ? 1 : null,
          data.BusinessType ?? null,
          data.WaterBodyName ?? null,
          data.View ?? null,
          data.NumberOfBuildings ?? null,
          data.NumberOfUnitsTotal ?? null,
          data.LotFeatures ?? null,
          data.LotSizeArea ?? null,
          data.LotSizeDimensions ?? null,
          data.LotSizeUnits ?? null,
          data.PoolFeatures ?? null,
          data.RoadSurfaceType ?? null,
          data.CurrentUse ?? null,
          data.PossibleUse ?? null,
          data.AnchorsCoTenants ?? null,
          data.WaterfrontFeatures ?? null,
          data.CommunityFeatures ?? null,
          data.Appliances ?? null,
          data.OtherEquipment ?? null,
          data.SecurityFeatures ?? null,
          data.TotalActualRent ?? null,
          data.ExistingLeaseType ?? null,
          data.AssociationFee ?? null,
          data.AssociationFeeFrequency ?? null,
          data.AssociationName ?? null,
          data.AssociationFeeIncludes ?? null,
          data.OriginalEntryTimestamp ?? null,
          data.ModificationTimestamp ?? null,
          data.ListingId ?? null,
          data.InternetEntireListingDisplayYN ?? null,
          data.StandardStatus ?? null,
          data.StatusChangeTimestamp ?? null,
          data.PublicRemarks ?? null,
          data.ListPrice ?? null,
          data.Inclusions ?? null,
          data.CoListOfficeKey ?? null,
          data.CoListAgentKey ?? null,
          data.ListAgentKey ?? null,
          data.InternetAddressDisplayYN ?? null,
          data.ListingURL ?? null,
          data.OriginatingSystemName ?? null,
          data.PhotosCount ?? null,
          data.PhotosChangeTimestamp ?? null,
          data.UnparsedAddress ?? null,
          data.PostalCode ?? null,
          data.SubdivisionName ?? null,
          data.StateOrProvince ?? null,
          data.StreetDirPrefix ?? null,
          data.StreetDirSuffix ?? null,
          data.StreetName ?? null,
          data.StreetNumber ?? null,
          data.StreetSuffix ?? null,
          data.UnitNumber ?? null,
          data.Country ?? null,
          data.City ?? null,
          data.Directions ?? null,
          data.Latitude ?? null,
          data.Longitude ?? null,
          data.CityRegion ?? null,
          data.ParkingTotal ?? null,
          data.YearBuilt ?? null,
          data.BathroomsPartial ?? null,
          data.BathroomsTotalInteger ?? null,
          data.BedroomsTotal ?? null,
          data.BuildingAreaTotal ?? null,
          data.BuildingAreaUnits ?? null,
          data.BuildingFeatures ?? null,
          data.AboveGradeFinishedArea ?? null,
          data.AboveGradeFinishedAreaUnits ?? null,
          data.LivingArea ?? null,
          data.LivingAreaUnits ?? null,
          data.FireplacesTotal ?? null,
          data.FireplaceYN ?? null,
          data.FireplaceFeatures ?? null,
          data.ArchitecturalStyle ?? null,
          data.Heating ?? null,
          data.FoundationDetails ?? null,
          data.Basement ?? null,
          data.ExteriorFeatures ?? null,
          data.Flooring ?? null,
          data.ParkingFeatures ?? null,
          data.Cooling ?? null,
          data.PropertyCondition ?? null,
          data.Roof ?? null,
          data.ConstructionMaterials ?? null,
          data.Stories ?? null,
          data.PropertyAttachedYN ?? null,
          data.Zoning ?? null,
          data.ZoningDescription ?? null,
          data.TaxAnnualAmount ?? null,
          data.TaxBlock ?? null,
          data.TaxLot ?? null,
          data.TaxYear ?? null,
          data.StructureType ?? null,
          data.Utilities ?? null,
          data.IrrigationSource ?? null,
          data.WaterSource ?? null,
          data.Sewer ?? null,
          data.Electric ?? null,
          data.CommonInterest ?? null,
          data.Media ?? null,
          data.Rooms ?? null,
          createdAt,
        ];
        return f;
      })
    );
    fs.writeFile("Output.txt", destinationsSpecificQuery, (err) => {
      // In case of a error throw err.
      if (err) throw err;
    });
    const res = await pool.query(destinationsSpecificQuery, (err, res) => {
      console.log(err);
    });
    pool.end();
  },
};
module.exports = dbService;

function getAgentName(data, id) {
  if (id) {
    const agent = data.find((d) => d.MemberKey === id);

    if (agent) return agent?.MemberFirstName + " " + agent?.MemberLastName;
  }
  return null;
}
