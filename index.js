const dbService = require("./dbService");
const externalService = require("./externalService");
require("dotenv").config();

(async () => {
  const token = await externalService.getToken();

  // ==================  //
  // populating the lists table

  let listsData = await externalService.getAllListData(token);

  // //   converting objects into strings to store in db
  // listsData = listsData.reduce((acc, obj) => {
  //   const updatedObj = {};
  //   Object.entries(obj).forEach(([key, value]) => {
  //     updatedObj[key] =
  //       Array.isArray(value) && value.length === 0
  //         ? "[]"
  //         : Array.isArray(value) && value.length !== 0
  //         ? JSON.stringify(value)
  //         : value;
  //   });
  //   return [...acc, updatedObj];
  // }, []);

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
    // const destRows = await dbService.cerateSeparateDestination(tableName);

    // get updated destination id from external api

    const destLiveData = await externalService.getDestinationData(
      token,
      dest.DestinationId
    );

    // if there is no data in table add all
    if (true || (destRows && destRows.length === 0)) {
      console.log("Inserting");
      const filteredArray = listsData.filter((data) => {
        return destLiveData.find(
          (lData) => data.ListingKey === lData.ListingKey
        );
      });

      await dbService.populateDestinationTable(
        tableName,
        filteredArray,
        dest.DestinationId,
        agentsData
      );
    }

    // listsData

    // console.log(destLiveData);
  }
})();
