const categories = require('./categories.json');

function findParent(cat) {
  const parent = categories.find(o => o.alias === cat).parents;
	if (parent.length === 0) return cat;
	if (parent.length === 1) parent.toString();
  // console.log(`${cat} --> ${parent}`);
  return findParent(parent[0]);
}

function getWeights(cats) {
  const weighted = [
    { category: 'restaurants', weight: 2 },
    { category: 'food', weight: 1.5 },
    { category: 'nightlife', weight: 1.5 }
  ];
  let weights = [];
  for (cat of cats) {
    const weight = weighted.map(o => o.category).includes(cat) ?
      weighted.find(o => o.category === cat).weight :
      1;

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

(() => {
  // Medical Centers, Obstetricians & Gynecologists, Adult Education
  // Private Tutors, Adult Education, Language Schools
  let string;
  string = 'Pubs, American (Traditional)';
  // const cats = findAliases('Kids Activities, Arcades, Pizza')
  console.log(`String:\t\t${string}`);
  cats = findAliases(string);
  console.log(`Categories:\t${cats}`)
  const parents = getParents(cats);
  console.log(`Parents:\t${parents}`);
  const weights = getWeights(parents);
  console.log(`Weights:`);
  console.log(weights);
  const heaviest = getHeaviest(weights);
  console.log(`Winner:\t${findName(heaviest)}`);
})();