const dbService = require("./dbService");
const externalService = require("./externalService");
require("dotenv").config();
let iteration = 0;

setTimeout(async () => {
  console.log("Started running", iteration);
  iteration++;

  await main();
  setInterval(async () => {
    console.log("Started running", iteration);
    iteration++;
    await main();
  }, 6000 * 10);
}, 0);
async function main() {
  const token = await externalService.getToken();
  // ==================  //
  // populating the lists table

  let listsData = await externalService.getAllListData(token);

  //   converting objects into strings to store in db
  listsData = formatDataForDB(listsData);

  //   adding "lists" data fetched from api to postgresql db
  await dbService.addAllListData(listsData);

  //   ==================== //
  // Now populating the listingagentid table-

  // getting data from external api

  const agentsData = await externalService.getAgentsListingData(token);
  await dbService.addListingAgentsData(agentsData);

  //   now inserting this data in db

  //   ==================== //
  //   Now populating destiantions table

  // TODO ask the purpose of is deleted in this table
  // TODO  can i remove extra fields like listingagentname colistingagentname etc
  const destinationsData = await externalService.getAllDestinations(token);

  await dbService.addDestinationsData(destinationsData);

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

    const destinationListingData = listsData.filter((data) => {
      const isPresent = destLiveData.find((lData) => {
        return data.ListingKey === lData.ListingKey;
      });
      if (isPresent) return data;
      else return false;
    });

    // if there is no data in table add all
    if (destRows.length === 0) {
      await dbService.populateDestinationTable(
        tableName,
        formatDataForDB(destinationListingData),
        dest.DestinationId,
        agentsData
      );
    } else {
      const newRows = [];
      await Promise.all(
        destinationListingData.map(async (listData) => {
          const dbListData = destRows.find(
            (d) => d.listingkey === listData.ListingKey
          );
          const modifications = { updatedat: new Date() };
          if (!dbListData) {
            // so if it not present in db it means its a new entry
            newRows.push(listData);
          } else {
            if (
              dbListData.modificationtimestamp != listData.ModificationTimestamp
            ) {
              // if the modification timestap is different than the one in db
              // it means its updates

              // we will check what is updated and will update accordingly in db

              if (dbListData.listprice != listData.ListPrice) {
                modifications.listprice = listData.ListPrice;
                modifications.listprice_old = dbListData.listprice;
              }

              if (dbListData.bedroomstotal != listData.BedroomsTotal) {
                modifications.bedroomstotal = listData.BedroomsTotal;
                modifications.bedroomstotal_old = dbListData.bedroomstotal;
              }

              if (dbListData.publicremarks != listData.PublicRemarks) {
                modifications.publicremarks = listData.BedroomsTotal;
                modifications.publicremarks_old = dbListData.bedroomstotal;
              }
              if (
                dbListData.bathroomstotalinteger !=
                listData.BathroomsTotalInteger
              ) {
                modifications.bathroomstotalinteger =
                  listData.BathroomsTotalInteger;
                modifications.bathroomstotalinteger_old =
                  dbListData.bathroomstotalinteger;
              }
              if (dbListData.buildingareatotal != listData.BuildingAreaTotal) {
                modifications.buildingareatotal = listData.BuildingAreaTotal;
                modifications.buildingareatotal_old =
                  dbListData.buildingareatotal;
              }
            }
          } //END MAIN IF

          // now we have all the modifications
          // console.table(Object.keys(modifications).length);
          if (Object.keys(modifications).length > 1) {
            await dbService.updateDestinationTable(
              tableName,
              listData.ListingKey,
              modifications
            );
          }
        })
      );
      if (newRows.length > 0) {
        await dbService.populateDestinationTable(
          tableName,
          formatDataForDB(newRows),
          dest.DestinationId,
          agentsData
        );
      }

      const deletedRows = [];
      // now marking deleted rows in db
      destRows.map(async (d) => {
        if (
          !destinationListingData.find(
            (listData) => d.listingkey === listData.ListingKey
          )
        ) {
          deletedRows.push(d.listingkey);
        }
      });
      if (deletedRows.length > 0) {
        await Promise.all(
          deletedRows.map(async (id) => {
            await dbService.updateDestinationTable(tableName, id, {
              isdeleted: "DELETED",
              updatedat: new Date(),
            });
          })
        );
      }
    }
  } // end for loop
}

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
