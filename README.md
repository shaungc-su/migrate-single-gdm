# How to migrate related objects for a single gdm
The scripts in this repo fetches old system data from postgres, and POST into DB using new system's server & dynamoDB.


### Step 1. Prepare the code
1. Make sure you're at root directory of this repo.
2. Create a virtual environment `python -m venv venv`.
3. Acticate venv `. ./venv/bin/activate`.
4. Install standard dependencies. Run `pip install -r requirements.txt`
5. Install dependencies of serverless code base. This will allow the script to reuse utilities from our gci/vci serverless code base.
    1. Make sure you're at root directory of this repo.
    2. Configure `GCI_VCI_SERVERLESS_RELATIVE_PATH=...` in `.env` - point it to the `gci-vci-serverless` (its repo root) directory relative to this repo.
    3. Run `. ./install-gci-vci-serverless-code.sh`.

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
  url: http://0.0.0.0:3000/ # the local serverless endpoint
  # url: https://xxxx.execute-api.us-west-2.amazonaws.com/xxx # or paste the AWS RDS postgres endpoint here
queries:
  - select item_type, rownum, item from (
    select row_number() over(order by rid, sid) as rownum, 
    item_type, item 
    from migrate_recent_items where item_type=%s
    )a  

```

- If you are considering setup a local postgres with production data, you can refer to the section "How to prepare a postgres with legacy production data" below.
- Make sure if you're using postgres on AWS RDS, change the following in `migrate_single_gdm.py`

```py
def getConnection():
  type = 'ec2' # or change to `local` if using a local postgres
```

### Step 3. Spin up a local dynamoDB
1. (Recommended) Create a directory `data` for db data at `gci-vci-aws/gci-vci-serverless/.dynamodb/data/`.
2. `cd` to `gci-vci-serverless` directory, and start DynamoDB local server: `npx nodemon --watch .dynamodb/data --ext db --exec 'npx serverless dynamodb start --migrate --dbPath ./.dynamodb/data'`. The `nodemon` will auto restart the DynamoDB local server whenever db data changed.
3. It's better to have a db with your user object already created, so at least you can login the UI.

### Step 4. Spin up local serverless dev server
1. Run `npx nodemon --ext py --exec "npx serverless offline --noTimeout"`

### Step 5. Run the script
1. Make sure you're at root directory of this repo.
2. Run `eval $(egrep -v '^#' .env | xargs) AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... python migrate_single_gdm.py`.

## Q&A

- Q: I want to reset my database
    - A: You can delete the db data file at `gci-vci-serverless/.dynamodb/data/shared-local-instance.db`, and restart the dynamodb dev server. A new db data file will be created with empty Tables.
    - A: (advance) If you want to do this programmatically, you can take a look at `utils/sls.py` the `class DynamoDB.reset()` method, and call it in `if __name__ == "__main__":` in `migrate_single_gdm.py`.
- Q: I want to change a GDM rid to collect objects.
    - A: Change the hard-coded `GDM_RID` value in file `migrate_single_gdm.py`.
- Q: I want to clear out all items in DyanmoDB table
    - A: If you're on local, simply delete the file `shared-local-instance.db` under `gci-vci-serverless/.dynamodb`
    - A: Or if you're on AWS cloud, the best way is to [drop and create the table](https://stackoverflow.com/a/51663200/9814131). Install & configure [AWS CLI tool](https://github.com/shaungc-su/awscli-profile-credential-helpers) first. Then you can refer to the file `scripts/reset_dynamodb_table.sh`, change `TABLE_NAME` and `AWS_PROFILE` to adapt to your config & credentials, then run the script.


# How to prepare a postgres with legacy production data

## Step 1: prepare a postgres instance
- Either on local or on AWS RDS, create a new postgres database instance.
- Also create a database in the instance with name, e.g. `prod_09152020`.
- If you are running AWS RDS database, make sure it is publicly accessible (go to db instance, press modify, and change the public accessibility setting), and the security group rules allows inbound rules of port `5432` and from any IP (or you can set to your IP your laptop network is currently using, ideally if your IP address provided by ISP is static, oftentime not though).

## Step 2: load production data from old website
- Go into s3 and download the sql file from s3 bucket `gcivcihelio`, e.g. the file `pg-dump-2020-09-15.sql.gz`. Download and de-compress it.
- Run `psql -U postgres -h <host> -d prod_09152020 -f <path to de-compressed sql file>`. The host is `localhost` if you're running local postgres, or in form of `db-instance-name.xxx.us-west-2.rds.amazonaws.com` if you're using AWS RDS.

## Step 3: transform some item_type in legacy system
- Connect to the postgres database `psql -U postgres -h <host> -d prod_09152020`
- Thanks to [Rao's work](https://github.com/ClinGen/gci-vci-aws/blob/fix/migration/migration/src/migrate_recent_items_vw.sql), run the following to transform the item_types. **Make sure the right hand side of lines `WHEN r.item_type::text = ...`, the item_type should match that in new system**. This will create a virtual table `migrate_recent_items` with the transformed item_types, and we can run sql query against this table. The virtual table data is persistent.
```sql
-- public.migrate_recent_items source
CREATE OR REPLACE VIEW public.migrate_recent_items
AS WITH all_recent_items AS (
         SELECT p_1.rid,
            p_1.sid,
                CASE
                    WHEN r.item_type::text = 'extra_evidence'::text THEN 'curated-evidence'::character varying
                    WHEN r.item_type::text = 'provisional_variant'::text THEN 'provisional-variant'::character varying
                    WHEN r.item_type::text = 'caseControl'::text THEN 'caseControl'::character varying
                    WHEN r.item_type::text = 'evidenceScore'::text THEN 'evidenceScore'::character varying
                    ELSE r.item_type
                END AS item_type,
            p_1.properties
           FROM propsheets p_1,
            resources r,
            current_propsheets cp
          WHERE cp.rid = p_1.rid AND cp.sid = p_1.sid AND r.rid = p_1.rid
        )
 SELECT p.rid,
    p.sid,
    p.item_type,
    (('{"body":'::text || (((((('{"rid":"'::text || p.rid) || '"}'::text)::jsonb) || ((('{"item_type":"'::text || p.item_type::text) || '"}'::text)::jsonb)) || p.properties)::text)) || '}'::text)::jsonb AS item
   FROM all_recent_items p;

```

## Step 4: Congrats - you now have a production data postgres database
You can now run the script above. Make sure your database connection config is set properly.