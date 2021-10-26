const csv = require("csv-parser");
const createCsvStringifier = require("csv-writer").createObjectCsvStringifier;
const fs = require("fs");
const Transform = require("stream").Transform;

const area = "nyc";
const state = "NY";

const categories = require('./categories.json');

const csvStringifier = createCsvStringifier({
  header: [
    { id: "name", title: "name" },
    { id: "display_phone", title: "phone" },
    { id: "categories", title: "categories" },
    { id: "mainCategory", title: "mainCategory" },
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
let writeStream = fs.createWriteStream(`businesses-${area}-filtered-categorized.csv`);

function removeNull(obj) {
  return Object.fromEntries(Object.entries(obj).filter(([_, v]) => v !== "null"));
}

function findParent(cat) {
  const parent = categories.find(o => o.alias === cat).parents;
	if (parent.length === 0) return cat;
	if (parent.length === 1) parent.toString();
  // console.log(`${cat} --> ${parent}`);
  return findParent(parent[0]);
}

function getWeights(cats) {
  const weighted = ['restaurants', 'food', 'nightlife'];
  let weights = [];
  for (cat of cats) {
    // cat = findName(cat);
    const weight = weighted.includes(cat) ? 2 : 1;
    const categoryExists = weights.find(o => o.category === cat);
    if (categoryExists && categoryExists.weight) {
      categoryExists.weight + weight;
    }
    else {
      weights.push({
        category: cat,
        weight: weight
      });
    }
  }
  return weights;
}

function getParents(cats) {
  const parents = [];
  for (cat of cats) {
    parents.push(findParent(cat));
  }
  return parents;
}

function findName(alias) {
  return categories.find(o => o.alias === alias).title;
}

function getHeaviest(weights) {
  const sorted = weights.sort(function(a, b) { return b.weight-a.weight });
  const max = sorted[0].weight;
  const atMax = sorted.filter(o => o.weight === max);
  const last = atMax[atMax.length-1];
  const first = atMax[0];
  if (atMax.length > 1) {
    const cats = atMax.map(o => o.category);
    // console.log(`[WARNING] Equal weights (${max}): ${cats.join(', ')}`);
    // console.log(`[WARNING] Using ${first.category}`);
  }
  return first.category;
}

function findAliases(str) {
  const titles = str.split(', ');
  const cats = [];
  for (title of titles) {
    cats.push(categories.find(o => o.title === title).alias);
  }
  return cats;
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
      const catsAll = JSON.parse(chunk.categories).map(x => x.title).reverse().join(", ");
      const cats = JSON.parse(chunk.categories).map(o => o.alias).reverse();
      const parents = getParents(cats);
      const weights = getWeights(parents);
      const heaviest = getHeaviest(weights);
      chunk.categories = catsAll;
      chunk.mainCategory = findName(heaviest);
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
