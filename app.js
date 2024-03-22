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
  headerIdDelimiter: '.',
});

let readStream = fs.createReadStream(`businesses-${area}-filtered.csv`);
let writeStream = fs.createWriteStream(`businesses-${area}-filtered-clean.csv`);

const transformObj = (obj) => {
  return Object.keys(obj).reduce((acc, key) => {
    if (key.includes('.')) {
      const [parentKey, childKey] = key.split('.')
      acc[parentKey] = acc[parentKey] || {}
      acc[parentKey][childKey] = obj[key]
    }
    else {
      acc[key] = obj[key]
    }
    return acc
  }, {})
}

const transformObj2 = (obj) => {
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

const keysToValues = obj => Object.keys(obj).map(key => obj[key])

const removeEmpty = (obj) => Object.fromEntries(Object.entries(obj).filter(([_, v]) => v != null))
const removeNull = (obj) => Object.fromEntries(Object.entries(obj).filter(([_, v]) => v !== "null"))

// https://stackoverflow.com/questions/286141/remove-blank-attributes-from-an-object-in-javascript/57625661#57625661
const removeFalsy = (obj) => Object.entries(obj).reduce((a,[k,v]) => (v ? (a[k]=v, a) : a), {})
const cleanEmpty = obj => Object.entries(obj)
  .map(([k,v])=>[k,v && typeof v === "object" ? cleanEmpty(v) : v])
  .reduce((a,[k,v]) => (v == null ? a : (a[k]=v, a)), {})

const isEmpty = (obj) => {
  for (const i in obj)
    return false
  return true
}

const isNum = (str) => /^\d+$/.test(str)

// pipeline(getDir, filterOdt, count)(dirname)
const pipe = (...fns) => fns.reduce((f, g) => (...args) => g(f(...args)))

class CSVCleaner extends Transform {
  constructor(options) {
    super(options);
  }

  _transform(chunk, encoding, next) {
    // console.log(`object_id: ${chunk["_id"]}`)
    if (
      chunk &&
      !isEmpty(chunk) &&
      chunk["location.state"] === state &&
      chunk["display_phone"] &&
      chunk["location.zip_code"].length >= 3 &&
      chunk["location.zip_code"].length <= 5
      ) {
      chunk["location.zip_code"].padStart(5, '0') // zips before NY

      // chunk = removeNull(chunk);
      chunk = removeFalsy(chunk)
      chunk = transformObj2(chunk)
      
      // chunk.categories = keysToValues(chunk.categories)
      chunk.categories = Object.keys(chunk.categories).map(key => chunk.categories[key])
      chunk.categories = chunk.categories.map(value => value.title).reverse().join('|')
      
      // chunk.county = counties.find(value => value.zip === chunk["location.zip_code"])?.county;
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
