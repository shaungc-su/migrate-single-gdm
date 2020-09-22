## How to migrate related objects for a single gdm
The scripts in this repo fetches old system data from postgres, and POST into DB using new system's server & dynamoDB.


### Step 1. Prepare the code
1. Make sure you're at root directory of this repo.
2. Create a virtual environment `python -m venv venv`.
3. Acticate venv `. ./venv/bin/activate`.
4. Install standard dependencies. Run `pip install -r requirements.txt`
5. Install dependencies of serverless code base. This will allow the script to reuse utilities from our gci/vci serverless code base.
    1. Make sure you're at root directory of this repo.
    2. Configure `GCI_VCI_SERVERLESS_RELATIVE_PATH` in shell script `install-gci-vci-serverless-code.sh` - point it to the `gci-vci-serverless/src` directory relative to this repo.
    2. Run `. ./install-gci-vci-serverless-code.sh`.

### Step 2. Get ready for AWS & Postgres
- AWS credentials are required for accessing postgres on AWS RDS. Specifically, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
- Make sure the postgres instance (migration) on AWS RDS is on and ready (not stopped).
- Create the config yaml file `config_recent.yaml` at root directory:

```yaml
db:
  ec2:
    user: <acquire-this-from-the-team>
    password: <acquire-this-from-the-team>
    host: <acquire-this-from-the-team>
    port: <acquire-this-from-the-team>
    database: <acquire-this-from-the-team>
  local: 
    user: your-db-user
    host: 127.0.0.1
    port: 5432
    database: your-db-name
endpoint:
  url: http://0.0.0.0:3000/ # the local serverless endpoint; you can 
queries:
  - select item_type, rownum, item from (
    select row_number() over(order by rid, sid) as rownum, 
    item_type, item 
    from migrate_recent_items where item_type=%s
    )a  

```

### Step 3. Spin up a local dynamoDB
1. (Recommended) Create a directory `data` for db data at `gci-vci-aws/gci-vci-serverless/.dynamodb/data/`.
2. `cd` to `gci-vci-serverless` directory, and start DynamoDB local server: `npx nodemon --watch .dynamodb/data --ext db --exec 'npx serverless dynamodb start --migrate --dbPath ./.dynamodb/data'`. The `nodemon` will auto restart the DynamoDB local server whenever db data changed.

### Step 4. Spin up local serverless dev server
1. Run `npx nodemon --ext py --exec "npx serverless offline --noTimeout"`

### Step 5. Run the script
1. Make sure you're at root directory of this repo.
2. Run `AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... python migrate_single_gdm.py`.

## Q&A

- Q: I want to reset my database
    - A: You can delete the db data file at `gci-vci-serverless/.dynamodb/data/shared-local-instance.db`, and restart the dynamodb dev server. A new db data file will be created with empty Tables.
    - A: (advance) If you want to do this programmatically, you can take a look at `utils/sls.py` the `class DynamoDB.reset()` method, and call it in `if __name__ == "__main__":` in `migrate_single_gdm.py`.
- Q: I want to change a GDM rid to collect objects.
    - A: Change the hard-coded `GDM_RID` value in file `migrate_single_gdm.py`.