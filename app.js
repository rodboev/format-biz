const csv = require("csv-parser");
const createCsvStringifier = require("csv-writer").createObjectCsvStringifier;
const fs = require("fs");
const Transform = require("stream").Transform;

const area = "nyc";
const state = "NY";

const csvStringifier = createCsvStringifier({
  header: [
    { id: "name", title: "name" },
    { id: "display_phone", title: "phone" },
    { id: "categories", title: "categories" },
    { id: "location.address1", title: "address1" },
    { id: "location.address2", title: "address2" },
    { id: "location.address3", title: "address3" },
    { id: "location.city", title: "city" },
    { id: "county", title: "county" },
    { id: "location.state", title: "state" },
    { id: "location.zip_code", title: "zip" },
    { id: "coordinates.latitude", title: "latitude" },
    { id: "coordinates.longitude", title: "longitude" },
  ],
});

let readStream = fs.createReadStream(`businesses-${area}-filtered.csv`);
let writeStream = fs.createWriteStream(`businesses-${area}-filtered-clean.csv`);

function removeNull(obj) {
  return Object.fromEntries(Object.entries(obj).filter(([_, v]) => v !== "null"));
}

class CSVCleaner extends Transform {
  constructor(options) {
    super(options);
  }

  _transform(chunk, encoding, next) {
    if (chunk["location.state"] === state && chunk.display_phone && chunk["location.zip_code"] && chunk["location.zip_code"].length === 5 &&
        (chunk["location.zip_code"].startsWith("0") || chunk["location.zip_code"].startsWith("1"))
      ) {
      chunk = removeNull(chunk);
      let categories = JSON.parse(chunk.categories).map(x => x.title).reverse().join(", ");
      chunk.categories = categories;
      // chunk.county = counties.find(x => x.zip === chunk["location.zip_code"])?.county;
      const chunkString = csvStringifier.stringifyRecords([chunk]);
      this.push(chunkString);
    }
    next();
  }
}

const transformer = new CSVCleaner({ writableObjectMode: true });

writeStream.write(csvStringifier.getHeaderString());

readStream
  .pipe(csv())
  .pipe(transformer)
  .pipe(writeStream)
  .on("finish", () => {
    console.log("Finished");
  });
