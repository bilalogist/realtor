const axios = require("axios");
var qs = require("qs");
const { Pool } = require("pg");
const format = require("pg-format");

async function asyncData() {
  let allData;
  const pool = new Pool({
    user: "andrew4a923a7dcef14a7d",
    host: "db.bit.io",
    database: "andrew4a923a7dcef14a7d/realstate",
    password: "v2_3x9Rb_XfDhkXHPt25YzMPsYBqhpQi",
    port: 5432,
    ssl: true,
  });
  let theKey = "";

  var data = qs.stringify({
    client_id: "yhSQ7WjLjP3dOA6XjQZ4GKT1",
    client_secret: "pYpUAJYPawk6s2b3eBtMWCMH",
    grant_type: "client_credentials",
    scope: "DDFApi_Read",
  });
  var config = {
    method: "post",
    url: "https://identity.crea.ca/connect/token",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Cookie:
        "ARRAffinity=da52619429e18bb2e3e580b87bc8cc0c281352ea518c1660bf604c7e1d105227; ARRAffinitySameSite=da52619429e18bb2e3e580b87bc8cc0c281352ea518c1660bf604c7e1d105227",
    },
    data: data,
  };

  axios(config)
    .then(function (response) {
      theKey = response.data.access_token;
      var config = {
        method: "get",
        url: "https://ddfapi.realtor.ca/odata/v1/Property?$top=100",
        headers: {
          Authorization: "Bearer " + theKey,
        },
      };

      axios(config)
        .then(function (response) {
          allData = response.data.value;
          let nextLink = response.data["@odata.nextLink"];

          pool.query("DELETE FROM lists;", (err, res) => {
            console.log(err);
            // console.log(res.rows);
          });

          pool.query("DELETE FROM listingagentids;", (err, res) => {
            console.log(err);
            // console.log(res.rows);
          });
          const createdAt = new Date();
          const listingAgentName = "test";

          allData = allData.reduce((acc, obj) => {
            const updatedObj = {};
            Object.entries(obj).forEach(([key, value]) => {
              updatedObj[key] =
                Array.isArray(value) && value.length === 0
                  ? "[]"
                  : Array.isArray(value) && value.length !== 0
                  ? JSON.stringify(value)
                  : value;
            });
            return [...acc, updatedObj];
          }, []);

          const listsQuery = format(
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

          pool.query(listsQuery, (err, res) => {
            if (err) console.error(err);
            else console.log(res.rows);
            // pool.end();
          });
          const uniqueKeys = new Set();

          const filteredArray = allData.filter((item) => {
            if (
              item.ListAgentKey !== null &&
              uniqueKeys.has(item.ListAgentKey)
            ) {
              return false;
            }
            if (
              item.CoListAgentKey !== null &&
              uniqueKeys.has(item.CoListAgentKey)
            )
              return false;
            if (item.ListAgentKey === null) item.ListAgentKey = null;
            else uniqueKeys.add(item.ListAgentKey);

            if (item.CoListAgentKey === null) item.CoListAgentKey = null;
            else uniqueKeys.add(item.CoListAgentKey);
            return true;
          });
          const listingagentidsQuery = format(
            "INSERT INTO listingagentids (listingagentkeys, colistingagentkeys, createdat) VALUES %L RETURNING listingagentkeys",
            filteredArray.map((data) => [
              data.ListAgentKey,
              data.CoListAgentKey,
              createdAt,
            ])
          );
          // Execute the query
          pool.query(listingagentidsQuery, (err, res) => {
            if (err) console.error(err);
            else console.log(res.rows);
          });

          // for (var i = 0; i < response.data.value.length; i++) {
          //   addAgentNames(theKey, allData[i].ListAgentKey, pool);
          //   if (allData[i].CoListAgentKey !== "") {
          //     addCoAgentNames(theKey, allData[i].CoListAgentKey, pool);
          //   }
          // }
          // setTimeout(function () {
          //   if (nextLink !== undefined && nextLink !== "") {
          //     addNextData(theKey, nextLink, pool);
          //   }
          // }, 75000);
        })
        .catch(function (error) {
          console.log(error);
        });

      var config1 = {
        method: "get",
        url: "https://ddfapi.realtor.ca/odata/v1/Destination",
        headers: {
          Authorization: "Bearer " + theKey,
        },
      };

      axios(config1)
        .then(function (response) {
          let theDestinations = response.data.value;

          for (var i = 0; i < response.data.value.length; i++) {
            const currentOne = theDestinations[i];
            pool.query(
              "SELECT * FROM destinations where DestinationId=" +
                theDestinations[i].DestinationId +
                ";",
              (err, res) => {
                if (res.rows.length !== 1) {
                  pool.query(
                    "INSERT INTO destinations (destinationid, destinationname, destinationurl, destinationtype, destinationstatus,memberfirstname,memberlastname,memberkey,originalentrytimestamp,modificationtimestamp) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)RETURNING destinationid",
                    [
                      currentOne.DestinationId,
                      currentOne.DestinationName,
                      currentOne.DestinationUrl,
                      currentOne.DestinationType,
                      currentOne.DestinationStatus,
                      currentOne.MemberFirstName,
                      currentOne.MemberLastName,
                      currentOne.MemberKey,
                      currentOne.OriginalEntryTimestamp,
                      currentOne.ModificationTimestamp,
                    ],
                    (err, res) => {}
                  );
                }
                pool.query(
                  "CREATE TABLE IF NOT EXISTS Des" +
                    currentOne.DestinationId +
                    "(listingkey bigint,listingagentname text,colistingagentname text,destinationid bigint ,listofficekey bigint,availabilitydate bigint,propertysubtype text,documentsavailable json,leaseamount double precision,leaseamountfrequency bigint,businesstype json,waterbodyname bigint,view json,numberofbuildings bigint,numberofunitstotal bigint,lotfeatures json,lotsizearea double precision,lotsizedimensions text,lotsizeunits text,poolfeatures json,roadsurfacetype json,currentuse json,possibleuse json,anchorscotenants bigint,waterfrontfeatures json,communityfeatures json,appliances json,otherequipment json,securityfeatures json,totalactualrent double precision,existingleasetype json,associationfee double precision,associationfeefrequency text,associationname text,associationfeeincludes json,originalentrytimestamp text,modificationtimestamp text,listingid bigint,internetentirelistingdisplayyn boolean,standardstatus text, statuschangetimestamp text, publicremarks text, publicremarks_old text,listprice bigint,listprice_old bigint,inclusions bigint,colistofficekey bigint,colistagentkey bigint,listagentkey bigint, internetaddressdisplayyn boolean,listingurl text,originatingsystemname text,photoscount bigint,photoschangetimestamp text,unparsedaddress text,postalcode text,subdivisionname bigint,stateorprovince text,streetdirprefix bigint,streetdirsuffix text,streetname text,streetnumber bigint,streetsuffix text,unitnumber text, country text, city text,directions bigint,latitude double precision,longitude double precision,cityregion text,parkingtotal bigint,yearbuilt bigint, bathroomspartial bigint,bathroomstotalinteger bigint,bathroomstotalinteger_old bigint, bedroomstotal bigint, bedroomstotal_old bigint, buildingareatotal bigint, buildingareatotal_old bigint,buildingareaunits text,buildingfeatures json,abovegradefinishedarea bigint,abovegradefinishedareaunits bigint,livingarea bigint,livingareaunits text,fireplacestotal bigint,fireplaceyn boolean,fireplacefeatures json,architecturalstyle json,heating json,foundationdetails json,basement json,exteriorfeatures json, flooring json,parkingfeatures json,cooling json,propertycondition json, roof json,constructionmaterials json,stories character varying,propertyattachedyn character varying,zoning character varying,zoningdescription character varying,taxannualamount double precision,taxblock bigint ,taxlot bigint, taxyear bigint, structuretype json,utilities json,irrigationsource json,watersource json,sewer json,electric json,commoninterest text,media json,rooms json,isdeleted text,createdat text,updatedat text)",
                  (err, res) => {
                    var config2 = {
                      method: "get",
                      url:
                        " https://ddfapi.realtor.ca/odata/v1/Property/PropertyReplication(DestinationId=" +
                        currentOne.DestinationId +
                        ")",
                      headers: {
                        Authorization: "Bearer " + theKey,
                      },
                    };

                    axios(config2)
                      .then(function (response) {
                        let theDestinationListings = response.data.value;

                        setTimeout(function () {
                          let allIds = "1";
                          for (var i = 0; i < response.data.value.length; i++) {
                            const theListingKeyIs =
                              theDestinationListings[i].ListingKey;
                            const currentDestinationTable =
                              "Des" + currentOne.DestinationId;
                            allIds =
                              allIds +
                              "," +
                              theDestinationListings[i].ListingKey;

                            addDestinationData(
                              theListingKeyIs,
                              currentDestinationTable,
                              pool,
                              currentOne.DestinationId,
                              theKey
                            );
                          }
                        }, 50000);
                        const desTable = "Des" + currentOne.DestinationId;
                        setTimeout(function () {
                          addisdeleted(theKey, pool, desTable);
                        }, 150000);
                      })
                      .catch(function (error) {
                        console.log(error);
                      });
                  }
                );
              }
            );
          }
        })
        .catch(function (error) {
          console.log(error);
        });
    })
    .catch(function (error) {
      console.log(error);
    });
}

//Adding or updating data to destinations tables
async function addDestinationData(
  theListingKeyIs,
  currentDestinationTable,
  pool,
  destinationId,
  theKey
) {
  const config2 = {
    method: "get",
    url: "https://ddfapi.realtor.ca/odata/v1/Property/" + theListingKeyIs,
    headers: {
      Authorization: "Bearer " + theKey,
    },
  };

  axios(config2)
    .then(function (response) {
      pool.query(
        "SELECT * FROM " +
          currentDestinationTable +
          " where listingkey=" +
          theListingKeyIs +
          ";",
        (err, res) => {
          const createdAt = new Date();
          if (res.rows.length === 0) {
            pool.query(
              "SELECT * FROM lists where listingkey=" + theListingKeyIs + ";",
              (err, res) => {
                if (res.rows.length === 1) {
                  let currentlisting = res.rows[0];
                  const listingagentidsQuery = format(
                    "insert into " +
                      currentDestinationTable +
                      " (listingkey,listingagentname,colistingagentname,destinationid, listofficekey, availabilitydate, propertysubtype, documentsavailable,leaseamount,leaseamountfrequency,businesstype,waterbodyname,view,numberofbuildings,numberofunitstotal,lotfeatures,lotsizearea,lotsizedimensions,lotsizeunits,poolfeatures,roadsurfacetype,currentuse,possibleuse,anchorscotenants,waterfrontfeatures,communityfeatures,appliances,otherequipment,securityfeatures,totalactualrent,existingleasetype,associationfee,associationfeefrequency,associationname,associationfeeincludes,originalentrytimestamp,modificationtimestamp,listingid,internetentirelistingdisplayyn,standardstatus,statuschangetimestamp,publicremarks,listprice,inclusions,colistofficekey,colistagentkey,listagentkey,internetaddressdisplayyn,listingurl,originatingsystemname,photoscount,photoschangetimestamp,unparsedaddress,postalcode,subdivisionname,stateorprovince,streetdirprefix,streetdirsuffix,streetname,streetnumber,streetsuffix,unitnumber,country,city,directions,latitude,longitude,cityregion,parkingtotal,yearbuilt,bathroomspartial,bathroomstotalinteger,bedroomstotal,buildingareatotal,buildingareaunits,buildingfeatures,abovegradefinishedarea,abovegradefinishedareaunits,livingarea,livingareaunits,fireplacestotal,fireplaceyn,fireplacefeatures,architecturalstyle,heating,foundationdetails,basement,exteriorfeatures,flooring,parkingfeatures,cooling,propertycondition,roof,constructionmaterials,stories,propertyattachedyn,zoning,zoningdescription,taxannualamount,taxblock,taxlot,taxyear,structuretype,utilities,irrigationsource,watersource,sewer,electric,commoninterest,media,rooms,createdat) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72,$73,$74,$75,$76,$77,$78,$79,$80,$81,$82,$83,$84,$85,$86,$87,$88,$89,$90,$91,$92,$93,$94,$95,$96,$97,$98,$99,$100,$101,$102,$103,$104,$105,$106,$107,$108,$109,$110,$111,$112) returning listingkey",
                    [
                      currentlisting.listingkey,
                      currentlisting.listingagentname,
                      currentlisting.colistingagentname,
                      currentlisting.listofficekey,
                      currentlisting.availabilitydate,
                      currentlisting.propertysubtype,
                      currentlisting.documentsavailable,
                      currentlisting.leaseamount,
                      currentlisting.leaseamountfrequency ? 1 : null,
                      currentlisting.businesstype,
                      currentlisting.waterbodyname,
                      currentlisting.view,
                      currentlisting.numberofbuildings,
                      currentlisting.numberofunitstotal,
                      currentlisting.lotfeatures,
                      currentlisting.lotsizearea,
                      currentlisting.lotsizedimensions,
                      currentlisting.lotsizeunits,
                      currentlisting.poolfeatures,
                      currentlisting.roadsurfacetype,
                      currentlisting.currentuse,
                      currentlisting.possibleuse,
                      currentlisting.anchorscotenants,
                      currentlisting.waterfrontfeatures,
                      currentlisting.communityfeatures,
                      currentlisting.appliances,
                      currentlisting.otherequipment,
                      currentlisting.securityfeatures,
                      currentlisting.totalactualrent,
                      currentlisting.existingleasetype,
                      currentlisting.associationfee,
                      currentlisting.associationfeefrequency,
                      currentlisting.associationname,
                      currentlisting.associationfeeincludes,
                      currentlisting.originalentrytimestamp,
                      currentlisting.modificationtimestamp,
                      currentlisting.listingid,
                      currentlisting.internetentirelistingdisplayyn,
                      currentlisting.standardstatus,
                      currentlisting.statuschangetimestamp,
                      currentlisting.publicremarks,
                      currentlisting.listprice,
                      currentlisting.inclusions,
                      currentlisting.colistofficekey,
                      currentlisting.colistagentkey,
                      currentlisting.listagentkey,
                      currentlisting.internetaddressdisplayyn,
                      currentlisting.listingurl,
                      currentlisting.originatingsystemname,
                      currentlisting.photoscount,
                      currentlisting.photoschangetimestamp,
                      currentlisting.unparsedaddress,
                      currentlisting.postalcode,
                      currentlisting.subdivisionname,
                      currentlisting.stateorprovince,
                      currentlisting.streetdirprefix,
                      currentlisting.streetdirsuffix,
                      currentlisting.streetname,
                      currentlisting.streetnumber,
                      currentlisting.streetsuffix,
                      currentlisting.unitnumber,
                      currentlisting.country,
                      currentlisting.city,
                      currentlisting.directions,
                      currentlisting.latitude,
                      currentlisting.longitude,
                      currentlisting.cityregion,
                      currentlisting.parkingtotal,
                      currentlisting.yearbuilt,
                      currentlisting.bathroomspartial,
                      currentlisting.bathroomstotalinteger,
                      currentlisting.bedroomstotal,
                      currentlisting.buildingareatotal,
                      currentlisting.buildingareaunits,
                      currentlisting.buildingfeatures,
                      currentlisting.abovegradefinishedarea,
                      currentlisting.abovegradefinishedareaunits,
                      currentlisting.livingarea,
                      currentlisting.livingareaunits,
                      currentlisting.fireplacestotal,
                      currentlisting.fireplaceyn,
                      currentlisting.fireplacefeatures,
                      currentlisting.architecturalstyle,
                      currentlisting.heating,
                      currentlisting.foundationdetails,
                      currentlisting.basement,
                      currentlisting.exteriorfeatures,
                      currentlisting.flooring,
                      currentlisting.parkingfeatures,
                      currentlisting.cooling,
                      currentlisting.propertycondition,
                      currentlisting.roof,
                      currentlisting.constructionmaterials,
                      currentlisting.stories,
                      currentlisting.propertyattachedyn,
                      currentlisting.zoning,
                      currentlisting.zoningdescription,
                      currentlisting.taxannualamount,
                      currentlisting.taxblock,
                      currentlisting.taxlot,
                      currentlisting.taxyear,
                      currentlisting.structuretype,
                      currentlisting.utilities,
                      currentlisting.irrigationsource,
                      currentlisting.watersource,
                      currentlisting.sewer,
                      currentlisting.electric,
                      currentlisting.commoninterest,
                      currentlisting.media,
                      currentlisting.rooms,
                      destinationId,
                      createdAt,
                    ]
                  );

                  pool.query(listingagentidsQuery, (err, res) => {
                    if (err) console.error(err);
                    else console.log(res.rows);
                  });
                }
              }
            );
          } else {
            if (res.rows.length === 1) {
              let currentlisting = res.rows[0];
              pool.query(
                "SELECT * FROM " +
                  currentDestinationTable +
                  " where listingkey=" +
                  theListingKeyIs +
                  ";",
                (err, res) => {
                  const olderData = res.rows[0];
                  setTimeout(function () {
                    pool.query(
                      "update  " +
                        currentDestinationTable +
                        " SET  listprice=$1,listprice_old=$2,bedroomstotal=$3,bedroomstotal_old=$4,publicremarks=$5,publicremarks_old=$6,bathroomstotalinteger=$7,bathroomstotalinteger_old=$8,buildingareatotal=$9,buildingareatotal_old=$10,updatedat=$11 where listingkey=" +
                        theListingKeyIs +
                        ";",
                      [
                        currentlisting.listprice,
                        olderData.listprice,
                        currentlisting.bedroomstotal,
                        olderData.bedroomstotal,
                        currentlisting.publicremarks,
                        olderData.publicremarks,
                        currentlisting.bathroomstotalinteger,
                        olderData.bathroomstotalinteger,
                        currentlisting.buildingareatotal,
                        olderData.buildingareatotal,
                        createdAt,
                      ],

                      (err, res) => {
                        // console.log(err)
                      }
                    );
                  }, 2000);
                }
              );
            }
          }
        }
      );
    })

    .catch(function (error) {
      console.log(error);

      pool.query(
        "update  " +
          currentDestinationTable +
          " SET  isdeleted=$1 where listingkey=" +
          theListingKeyIs +
          ";",
        ["DELETED"],
        (err, res) => {
          console.log("update deleted requested");
          console.log(theListingKeyIs);
          console.log(err);
        }
      );
    });
}
async function addAgentNames(theKey, ListAgentKey, pool) {
  const config1 = {
    method: "get",
    url: "https://ddfapi.realtor.ca/odata/v1/Member/" + ListAgentKey,
    headers: {
      Authorization: "Bearer " + theKey,
    },
  };

  axios(config1)
    .then(function (response) {
      const memberData = response.data;
      const listingAgentName =
        memberData.MemberFirstName + " " + memberData.MemberLastName;
      pool.query(
        "update  lists SET  listingagentname=$1 where listagentkey=" +
          ListAgentKey +
          ";",
        [listingAgentName],
        (err, res) => {
          console.log("updated is");
          console.log(err);
        }
      );
    })

    .catch(function (error) {
      console.log(error.code);
    });
}

async function addCoAgentNames(theKey, ListAgentKey, pool) {
  const config1 = {
    method: "get",
    url: "https://ddfapi.realtor.ca/odata/v1/Member/" + ListAgentKey,
    headers: {
      Authorization: "Bearer " + theKey,
    },
  };

  axios(config1)
    .then(function (response) {
      const memberData = response.data;
      const listingAgentName =
        memberData.MemberFirstName + " " + memberData.MemberLastName;
      pool.query(
        "update  lists SET  colistingagentname=$1 where colistagentkey=" +
          ListAgentKey +
          ";",
        [listingAgentName],
        (err, res) => {
          console.log("updated is co listing name");
          console.log(err);
        }
      );
    })

    .catch(function (error) {
      console.log(error.code);
    });
}

async function addNextData(theKey, nextLink, pool) {
  var config = {
    method: "get",
    url: nextLink,
    headers: {
      Authorization: "Bearer " + theKey,
    },
  };

  axios(config)
    .then(function (response) {
      allData = response.data.value;
      const createdAt = new Date();
      const listsQuery = format(
        "INSERT INTO lists (listingkey, listofficekey, availabilitydate, propertysubtype, documentsavailable,leaseamount,leaseamountfrequency,businesstype,waterbodyname,view,numberofbuildings,numberofunitstotal,lotfeatures,lotsizearea,lotsizedimensions,lotsizeunits,poolfeatures,roadsurfacetype,currentuse,possibleuse,anchorscotenants,waterfrontfeatures,communityfeatures,appliances,otherequipment,securityfeatures,totalactualrent,existingleasetype,associationfee,associationfeefrequency,associationname,associationfeeincludes,originalentrytimestamp,modificationtimestamp,listingid,internetentirelistingdisplayyn,standardstatus,statuschangetimestamp,publicremarks,listprice,inclusions,colistofficekey,colistagentkey,listagentkey,internetaddressdisplayyn,listingurl,originatingsystemname,photoscount,photoschangetimestamp,unparsedaddress,postalcode,subdivisionname,stateorprovince,streetdirprefix,streetdirsuffix,streetname,streetnumber,streetsuffix,unitnumber,country,city,directions,latitude,longitude,cityregion,parkingtotal,yearbuilt,bathroomspartial,bathroomstotalinteger,bedroomstotal,buildingareatotal,buildingareaunits,buildingfeatures,abovegradefinishedarea,abovegradefinishedareaunits,livingarea,livingareaunits,fireplacestotal,fireplaceyn,fireplacefeatures,architecturalstyle,heating,foundationdetails,basement,exteriorfeatures,flooring,parkingfeatures,cooling,propertycondition,roof,constructionmaterials,stories,propertyattachedyn,zoning,zoningdescription,taxannualamount,taxblock,taxlot,taxyear,structuretype,utilities,irrigationsource,watersource,sewer,electric,commoninterest,media,rooms,createdat) VALUES %L RETURNING listingkey",
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
          createdAt,
        ])
      );

      pool.query(listsQuery, (err, res) => {
        if (err) console.error(err);
        else console.log(res.rows);
        // pool.end();
      });
      const listingagentidsQuery = format(
        "INSERT INTO listingagentids (listingagentkeys, colistingagentkeys, createdat) VALUES %L RETURNING listingagentkeys",
        allData.map((data) => [
          data.ListAgentKey,
          data.CoListAgentKey,
          createdAt,
        ])
      );

      pool.query(listingagentidsQuery, (err, res) => {
        if (err) console.error(err);
        else console.log(res.rows);
        // pool.end();
      });

      for (var i = 0; i < response.data.value.length; i++) {
        addAgentNames(theKey, allData[i].ListAgentKey, pool);
        if (allData[i].CoListAgentKey !== "") {
          addCoAgentNames(theKey, allData[i].CoListAgentKey, pool);
        }
      }
    })
    .catch(function (error) {
      console.log(error);
    });
}

async function addisdeleted(theKey, pool, desTable) {
  pool.query("SELECT * FROM " + desTable + ";", (err, res) => {
    console.log(err);
    if (res.rows.length > 0) {
      for (var i = 0; i < res.rows.length; i++) {
        const theListingKey = res.rows[i].listingkey;
        const config2 = {
          method: "get",
          url: "https://ddfapi.realtor.ca/odata/v1/Property/" + theListingKey,
          headers: {
            Authorization: "Bearer " + theKey,
          },
        };
        axios(config2)
          .then(function (response) {
            if (response && !response.data.ListingKey) {
              pool.query(
                "update  " +
                  desTable +
                  " SET  isdeleted=$1 where listingkey=" +
                  theListingKey +
                  ";",
                ["DELETED"],
                (err, res) => {
                  console.log("update deleted");
                  console.log(err);
                }
              );
              console.clear();
              console.log("Kye Deleted ", theListingKey);
            }
          })
          .catch(function (error) {});
      }
    }
  });
}

exports.InsertDataToDB = () => {
  asyncData();
};