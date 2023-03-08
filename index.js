const dbService = require("./dbService");
const externalService = require("./externalService");
require("dotenv").config();
const fs = require("fs");
const listsData = require("./listData.json");

(async () => {
  const token = await externalService.getToken();
  // ==================  //
  // populating the lists table

  // let listsData = await externalService.getAllListData(token);

  // //   converting objects into strings to store in db
  // listsData = formatDataForDB(listsData);

  // //   //   adding "lists" data fetched from api to postgresql db
  // await dbService.addAllListData(listsData);

  // //   ==================== //
  // // Now populating the listingagentid table-

  // // getting data from external api

  const agentsData = await externalService.getAgentsListingData(token);
  // await dbService.addListingAgentsData(agentsData);

  // // console.log(agentsData);
  // //   now inserting this data in db

  // //   ==================== //
  // //   Now populating destiantions table

  // // TODO ask the purpose of is deleted in this table
  // // TODO  can i remove extra fields like listingagentname colistingagentname etc
  const destinationsData = await externalService.getAllDestinations(token);

  // await dbService.addDestinationsData(destinationsData);

  //   ==================== //

  // create separate table for each destination

  for (let index = 0; index < destinationsData.length; index++) {
    const dest = destinationsData[index];
    const tableName = `des${dest.DestinationId}`;

    // create separate destination table and if it exists return its rows
    const destRows = await dbService.createSeparateDestination(tableName);

    // get updated destination id from external api

    const destLiveData = await externalService.getDestinationData(
      token,
      dest.DestinationId
    );

    // if there is no data in table add all
    if (destRows && destRows.length === 0) {
      console.log("Inserting");
      const filteredArray = listsData.filter((data) => {
        const isPresent = destLiveData.find((lData) => {
          return data.ListingKey === lData.ListingKey;
        });
        if (isPresent) return data;
        else return false;
      });

      console.log("Length", filteredArray.length);
      // console.table(filteredArray.map((d) => ({ ListingKey: d.ListingKey })));
      // await dbService.populateDestinationTable(
      //   tableName,
      //   formatDataForDB(filteredArray),
      //   dest.DestinationId,
      //   agentsData
      // );
    }

    // listsData

    // console.log(destLiveData);
  }
})();

function formatDataForDB(data) {
  return data.reduce((acc, obj) => {
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
}
