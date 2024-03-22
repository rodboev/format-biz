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
    { id: "location.county", title: "county" },
    { id: "location.state", title: "state" },
    { id: "location.zip_code", title: "zip" },
    { id: "coordinates.latitude", title: "latitude" },
    { id: "coordinates.longitude", title: "longitude" },
  ],
});

let readStream = fs.createReadStream(`businesses-${area}-filtered.csv`);
let writeStream = fs.createWriteStream(`businesses-${area}-filtered-clean.csv`);

const transformObj = (obj) => {
  Object.keys(obj).forEach(k => {
    const path = k.replace(/\[/g, '.').replace(/\]/g, '').split('.'),
      last = path.pop()

    if (path.length) {
      path.reduce((o, p) => o[p] = o[p] || {}, obj)[last] = obj[k]
      delete obj[k]
    }
  })
  return obj
}

const removeEmpty = (obj) => Object.fromEntries(Object.entries(obj).filter(([_, v]) => v != null))
const removeNull = (obj) => Object.fromEntries(Object.entries(obj).filter(([_, v]) => v !== "null"))
const removeFalsy = (obj) => Object.entries(obj).reduce((a,[k,v]) => (v ? (a[k]=v, a) : a), {})

const isEmpty = (obj) => {
  for (const i in obj)
    return false
  return true
}

// pipeline(getDir, filterOdt, count)(dirname)
const pipe = (...fns) => fns.reduce((f, g) => (...args) => g(f(...args)))

class CSVCleaner extends Transform {
  constructor(options) {
    super(options);
  }

  _transform(chunk, encoding, next) {
    console.log(`object_id: ${chunk["_id"]}`)
    if (
      chunk &&
      !isEmpty(chunk) &&
      chunk["location.state"] === state &&
      chunk["display_phone"].length > 0 &&
      chunk["location.zip_code"] &&
      chunk["location.zip_code"].length >= 4
      ) {
      chunk["location.zip_code"].padStart(5, '0') // zips before NY
      chunk = removeFalsy(chunk)
      chunk = transformObj(chunk)
      
      let categories = JSON.parse(chunk.categories).map((x) => { x.title }).reverse().join("|");
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
