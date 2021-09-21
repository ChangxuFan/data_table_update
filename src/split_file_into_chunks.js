const {chain} = require('stream-chain');
const {parser} = require('stream-json');
const {streamValues} = require('stream-json/streamers/StreamValues');
const fs = require('fs');

const BATCH_SIZE = 50000;

async function main() {
  await run();
}

async function run() {
  return new Promise((resolve, reject) => {
    const pipeline = chain([
      fs.createReadStream(__dirname + '/updated.json'),
      parser(),
      streamValues(),
      data => {
        const value = data.value;
        return value;
      }
    ]);
    let entries = []

    pipeline.on('data', (d) => {
      entries.push(d);
    });
    pipeline.on('end', () => {
      let i,
        j,
        temparray,
        batches = [];
      for (i = 0, j = entries.length; i < j; i += BATCH_SIZE) {
        temparray = entries.slice(i, i + BATCH_SIZE);
        writeFile(`${__dirname}/chunks/${i + 1}.json`, JSON.stringify(temparray));
      }
      resolve();
    });

  })
}
const writeFile = (filePath, fileContent) => {
  return new Promise((resolve, reject) => {
    fs.writeFile(filePath, fileContent, writeFileError => {
      if (writeFileError) {
        reject(writeFileError);
        return;
      }

      resolve(filePath);
    });
  });
}

main();