const categories = require('./categories.json');

function findParent(cat) {
  const parent = categories.find(o => o.alias === cat).parents;
	if (parent.length === 0) return cat;
  console.log(`${cat} --> ${parent}`);
	// if (parent.length === 1) return parent.toString();
  return findParent(parent[0]);
}

function getWeights(cats) {
  const weighted = [
    { category: 'restaurants', weight: 1.5 },
    { category: 'food', weight: 1.5 },
    { category: 'nightlife', weight: 1.5 },
    { category: 'venues', weight: 1.5 },
    // { category: 'localservices', weight: 0.9 },
  ];
  let weights = [];
  for (cat of cats) {
    const weight = weighted.map(o => o.category).includes(cat) ?
      weighted.find(o => o.category === cat).weight :
      1;

   const categoryExists = weights.find(o => o.category === cat);
    if (categoryExists && categoryExists.weight) {
      categoryExists.weight += weight;
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
    console.log(`[WARNING] Equal weights (${max}): ${cats.join(', ')}`);
    console.log(`[WARNING] Using ${first.category}`);
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

const pipeline = (...fns) => fns.reduce((f, g) => (...args) => g(f(...args)));

(() => {
  const locations = [
    {
      name: 'Tryon Public House',
      categories: 'Bars, Gastropubs, American (Traditional)',
    },
    {
      name: 'Bronx Alehouse',
      categories: 'Pubs, American (Traditional)',
    },
    {
      name: 'Accent Reduction and ESL for Business',
      categories: 'Private Tutors, Adult Education, Language Schools',
    },
    {
      name: 'Glam House Bx',
      categories: 'Party & Event Planning, Venues & Event Spaces, Party Equipment Rentals'
    },
    {
      name: 'Lincoln Technical Institute - Queens',
      categories: 'Colleges & Universities, Adult Education, Vocational & Technical School'
    },
    {
      name: 'Samuel Field Y',
      categories: 'Preschools, Community Service/Non-Profit, Child Care & Day Care'
    },
    {
      name: 'Bohemian Hall & Beer Garden',
      categories: 'Beer Gardens, Venues & Event Spaces',
    },
    {
      name: 'Hotel 64',
      categories: 'Hotels',
    },
    {
      name: 'Columbia Ob/Gyn Uptown',
      categories: 'Medical Centers, Obstetricians & Gynecologists, Adult Education'
    },
  ]
  for (location of locations) {
    console.log(`Name:\t\t${location.name}`);
    console.log(`Categories:\t${location.categories}`);
    const winner = pipeline(findAliases, getParents, getWeights, getHeaviest)(location.categories);
    console.log(`Main parent:\t${findName(winner)}\n`);
  }
})();