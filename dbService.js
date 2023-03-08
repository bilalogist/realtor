const { Pool } = require("pg");
const format = require("pg-format");
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

  createSeparateDestination: async (tableName) => {
    const pool = new Pool({
      user: process.env.DB_USER,
      host: process.env.DB_HOST,
      database: process.env.DB_DATABASE,
      password: process.env.DB_PASSWORD,
      port: process.env.DB_PORT,
      ssl: true,
    });
    pool.query(
      `CREATE TABLE IF NOT EXISTS ${tableName} (listingkey bigint NULL, listingagentname text NULL, colistingagentname text NULL, destinationid bigint  NULL, listofficekey bigint NULL, availabilitydate bigint NULL, propertysubtype text NULL, documentsavailable json NULL, leaseamount double precision NULL, leaseamountfrequency bigint NULL, businesstype json NULL, waterbodyname bigint NULL, view json NULL, numberofbuildings bigint NULL, numberofunitstotal bigint NULL, lotfeatures json NULL, lotsizearea double precision NULL, lotsizedimensions text NULL, lotsizeunits text NULL, poolfeatures json NULL, roadsurfacetype json NULL, currentuse json NULL, possibleuse json NULL, anchorscotenants bigint NULL, waterfrontfeatures json NULL, communityfeatures json NULL, appliances json NULL, otherequipment json NULL, securityfeatures json NULL, totalactualrent double precision NULL, existingleasetype json NULL, associationfee double precision NULL, associationfeefrequency text NULL, associationname text NULL, associationfeeincludes json NULL, originalentrytimestamp text NULL, modificationtimestamp text NULL, listingid bigint NULL, internetentirelistingdisplayyn boolean NULL, standardstatus text NULL,  statuschangetimestamp text NULL,  publicremarks text NULL,  publicremarks_old text NULL, listprice bigint NULL, listprice_old bigint NULL, inclusions bigint NULL, colistofficekey bigint NULL, colistagentkey bigint NULL, listagentkey bigint NULL,  internetaddressdisplayyn boolean NULL, listingurl text NULL, originatingsystemname text NULL, photoscount bigint NULL, photoschangetimestamp text NULL, unparsedaddress text NULL, postalcode text NULL, subdivisionname bigint NULL, stateorprovince text NULL, streetdirprefix bigint NULL, streetdirsuffix text NULL, streetname text NULL, streetnumber bigint NULL, streetsuffix text NULL, unitnumber text NULL,  country text NULL,  city text NULL, directions bigint NULL, latitude double precision NULL, longitude double precision NULL, cityregion text NULL, parkingtotal bigint NULL, yearbuilt bigint NULL,  bathroomspartial bigint NULL, bathroomstotalinteger bigint NULL, bathroomstotalinteger_old bigint NULL,  bedroomstotal bigint NULL,  bedroomstotal_old bigint NULL,  buildingareatotal bigint NULL,  buildingareatotal_old bigint NULL, buildingareaunits text NULL, buildingfeatures json NULL, abovegradefinishedarea bigint NULL, abovegradefinishedareaunits bigint NULL, livingarea bigint NULL, livingareaunits text NULL, fireplacestotal bigint NULL, fireplaceyn boolean NULL, fireplacefeatures json NULL, architecturalstyle json NULL, heating json NULL, foundationdetails json NULL, basement json NULL, exteriorfeatures json NULL,  flooring json NULL, parkingfeatures json NULL, cooling json NULL, propertycondition json NULL,  roof json NULL, constructionmaterials json NULL, stories character varying NULL, propertyattachedyn character varying NULL, zoning character varying NULL, zoningdescription character varying NULL, taxannualamount double precision NULL, taxblock bigint  NULL, taxlot bigint NULL,  taxyear bigint NULL,  structuretype json NULL, utilities json NULL, irrigationsource json NULL, watersource json NULL, sewer json NULL, electric json NULL, commoninterest text NULL, media json NULL, rooms json NULL, isdeleted text NULL, createdat text NULL, updatedat text NULL)`,
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
    console.log("Started execution", tableName);
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

    const res = await pool.query(destinationsSpecificQuery, (err, res) => {
      if (err) {
        console.log("Error occured while populating", allData, err);
        throw new Error(err);
      }
    });
    pool.end();
  },
  updateDestinationTable: async (tableName, listingkey, modifications) => {
    const pool = new Pool({
      user: process.env.DB_USER,
      host: process.env.DB_HOST,
      database: process.env.DB_DATABASE,
      password: process.env.DB_PASSWORD,
      port: process.env.DB_PORT,
      ssl: true,
    });

    let modNames = "";

    Object.keys(modifications).map((key, index, arr) => {
      modNames += `${key}=$${index + 1}${
        index + 1 === arr.length ? " " : ", "
      }`;
    });
    const modData = Object.keys(modifications).map((key, index) => {
      return modifications[key];
    });

    console.log("==========");
    console.log(modNames);
    console.table(modData);

    await pool.query(
      `UPDATE ${tableName} SET ${modNames} where listingkey=${listingkey};`,
      modData,
      (err, res) => {
        if (err) {
          console.log("error occured here", err);
          console.log(
            `UPDATE ${tableName} SET ${modNames} where listingkey=${listingkey};`,
            modData
          );
          throw new Error(err);
        }
      }
    );
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
