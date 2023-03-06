const dbService = require("./dbService");
const externalService = require("./externalService");
require("dotenv").config();
(async () => {
  const token = await externalService.getToken();

  // ==================  //
  // populating the lists table

  let listsData = await externalService.getAllListData(token);

  //   converting objects into strings to store in db
  listsData = listsData.reduce((acc, obj) => {
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

  //   //   adding "lists" data fetched from api to postgresql db
  await dbService.addAllListData(listsData);

  //   ==================== //
  // Now populating the listingagentid table

  // getting data from external api

  const agentsData = await externalService.getAgentsListingData(token);
  await dbService.addListingAgentsData(agentsData);

  // console.log(agentsData);
  //   now inserting this data in db

  //   ==================== //
  //   Now populating destiantions table

  // TODO ask the purpose of is deleted in this table
  // TODO  can i remove extra fields like listingagentname colistingagentname etc
  const destinationsData = await externalService.getDestinationsData(token);

  await dbService.addDestinationsData(destinationsData);
})();
