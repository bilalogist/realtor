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

    await pool.query(listsInsertQuery, (err, res) => {
      if (err) console.error("while inserting in lists", err);
      else console.log(res.rows);
      pool.end();
    });
  },
  addListingAgentsData: async (agentsData) => {
    const createdAt = new Date();
    const npool = new Pool({
      user: process.env.DB_USER,
      host: process.env.DB_HOST,
      database: process.env.DB_DATABASE,
      password: process.env.DB_PASSWORD,
      port: process.env.DB_PORT,
      ssl: true,
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
    npool.query(listingagentidsQuery, (err, res) => {
      if (err) console.error(err);
      else console.log(res.rows);
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
      else console.log(res);
    });
  },
};
module.exports = dbService;
