
debugpoint136/data_table_update
log in to HTCF and do below actions

```bash
node -v
```

if you see a message saying node is not available, please contact to install node first before proceeding

```bash
git clone git@github.com:debugpoint136/data_table_update.git

cd data_table_update

npm install
```

node split_file_into_chunks.js

- has a hardcoded file name "updated.json" <- this contains the update to be performed

```bash
[
    {
        "name": "EPI_ISL_426900",
        "type": "pairwise",
        "url": "https://wangftp.wustl.edu/~cfan/gisaid/2021-04-27/SNV/EPI_ISL_426900.bed.gz",
        "metadata": {
            "virus": "nCoV"
        }
    },
    {
        "name": "EPI_ISL_426901",
        "type": "pairwise",
        "url": "https://wangftp.wustl.edu/~cfan/gisaid/2021-04-27/SNV/EPI_ISL_426901.bed.gz",
        "metadata": {
            "virus": "nCoV"
        }
    },
    ....
```

```bash
mkdir chunks
mkdir commands
mkdir result
```

Ensure above folders have write permissions

in the next script, there is config block


```bash
node src/split_file_into_chunks.js
```

this will do the following - 
1. split `updated.json` to chunks and save in `chunks` folder



```javascript
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
```
compare the above with `updated.json` file to understand the keys that will get updated


```bash
node generate_bash_scripts.js
```

This script will take one chunk from `/chunks` folder at a time and generate POST request bash scripts and save in `/commands` folder


Finally, run the generated bash scripts as below

```bash
bash ./results/<auto complete with auto_generated>/run.sh
```

results folder should get populated with json which contain success or error reports of update process

