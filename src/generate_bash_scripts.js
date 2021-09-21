const fs = require('fs');
const path = require('path');
const APPROOT = require('app-root-path');
const {nanoid} = require('nanoid')
const ESCLUSTER = 'http://XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX.us-east-1.es.amazonaws.com'; // update this with real one
const INDEX = 'virusgateway';
const TYPE = 'file';

const UPDATE_KEYS = {
  lookup_from_json: {
    _id: 'name',
    URL: 'url'
  },
  hardcoded: {
    last_updated: new Date().toDateString(),
    updated: true,
    updated_by: 'Fan'
  }
}

/*************************************/

const testFolder = APPROOT + '/chunks/';
const POST_NUM = 5;

fs
  .readdirSync(testFolder)
  .slice(0, 1) // REMOVE after test
  .forEach(file => {
    start(file);
  });

async function start(file) {
  const chunkData = require(`${testFolder}${file}`);
  const chunkTest = chunkData.slice(0, 50) // REMOVE after test

  const runid = nanoid();
  fs.mkdir(APPROOT + '/commands/' + runid, (err => {
    if (err) 
      console.log(err);
    }
  ));
  fs.mkdir(APPROOT + '/results/' + runid, (err => {
    if (err) 
      console.log(err);
    }
  ));
  let i,
    j,
    temparray,
    cmds = [];
  for (i = 0, j = chunkTest.length; i < j; i += POST_NUM) {
    temparray = chunkTest.slice(i, i + POST_NUM); // THIS IS OK
    const postCmd = makePostCmdBody(temparray)
    fs.writeFileSync(`${APPROOT}/commands/${runid}/${file}.${i + 1}.sh`, postCmd);
    cmds.push(`bash ${APPROOT}/commands/${runid}/${file}.${i + 1}.sh > ${APPROOT}/results/${runid}/${file}.${i + 1}.json`);
  }
  fs.writeFileSync(`${APPROOT}/results/${runid}/run.sh`, cmds.join('\n'));
}

function makePostCmdBody(c) {

  return `
curl -s -H "Content-Type: application/x-ndjson" ${ESCLUSTER}/${INDEX}/${TYPE}/_bulk?pretty=true --data-binary '
  ${c
    .map(d => printPostRequest(d))
    .join('')}
'
  `;

  function printPostRequest(d) {
    return `
{ "update" : { ${getID(d)} } }
{ "doc":  ${getUpdateLookupString(d)} } 
`;
  }

  function getID(d) {
    const {lookup_from_json} = UPDATE_KEYS;

    let val = {};
    val["_id"] = d[lookup_from_json["_id"]];

    const str = JSON.stringify(val);
    const result = str.substr(1, str.length - 2);

    return result;
  }

  function getUpdateLookupString(d) {
    const {lookup_from_json, hardcoded} = UPDATE_KEYS;
    let res = {}

    Object
      .keys(lookup_from_json)
      .forEach(key => {
        if (key !== '_id') {
          res[key] = d[lookup_from_json[key]];
        }
      })

    Object
      .keys(hardcoded)
      .forEach(key => {
        res[key] = hardcoded[key];
      })
    const str = JSON.stringify(res);

    return str;
  }
}

start();