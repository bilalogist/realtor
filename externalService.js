const axios = require("axios");
// let token = false;
const BASE_URL = "https://ddfapi.realtor.ca/odata/v1";

const externalService = {
  // token: false,
  getToken: async () => {
    let config = {
      method: "post",
      url: "https://identity.crea.ca/connect/token",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded", // Cookie: // "ARRAffinity=da52619429e18bb2e3e580b87bc8cc0c281352ea518c1660bf604c7e1d105227; ARRAffinitySameSite=da52619429e18bb2e3e580b87bc8cc0c281352ea518c1660bf604c7e1d105227"
      },
      data: {
        client_id: process.env.CLIENT_ID,
        client_secret: process.env.CLIENT_SECRET,
        grant_type: process.env.GRANT_TYPE,
        scope: process.env.SCOPE,
      },
    };
    const response = await axios(config);
    return response?.data?.access_token;
  },

  getAllListData: async (token, pageSize = 100, skip = 0) => {
    try {
      console.log(pageSize, skip);
      let allData = [];
      let config = {
        method: "get",
        url: BASE_URL + `/Property?$top=${pageSize}&$skip=${skip}`,
        headers: {
          Authorization: "Bearer " + token,
        },
      };

      const response = await axios(config);
      const data = response?.data?.value;

      if (data) allData = [...allData, ...data];

      let nextLink = response?.data["@odata.nextLink"];

      if (nextLink) {
        const recursiveData = await externalService.getAllListData(
          token,
          pageSize,
          skip + pageSize
        );
        allData = [...allData, ...recursiveData];
      }

      return allData;
    } catch (err) {
      console.log("Error occured>>>", err);
    }
  },
  getDestinationsData: async (token) => {
    try {
      let config = {
        method: "get",
        url: BASE_URL + `/Destination`,
        headers: {
          Authorization: "Bearer " + token,
        },
      };

      const response = await axios(config);
      const data = response?.data?.value;

      return data;
    } catch (err) {
      console.log("Error occured>>>", err);
    }
  },
  getAgentsListingData: async (token) => {
    try {
      let config = {
        method: "get",
        url: BASE_URL + `/Member`,
        headers: {
          Authorization: "Bearer " + token,
        },
      };

      const response = await axios(config);
      const data = response?.data?.value;

      return data;
    } catch (err) {
      console.log("Error occured>>>", err);
    }
  },
};

module.exports = externalService;
