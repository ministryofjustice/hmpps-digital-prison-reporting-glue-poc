# Ministry of Justice Digital Prison Reporting Glue Scripts

[![repo standards badge](https://img.shields.io/badge/dynamic/json?color=blue&style=for-the-badge&logo=github&label=MoJ%20Compliant&query=%24.result&url=https%3A%2F%2Foperations-engineering-reports.cloud-platform.service.justice.gov.uk%2Fapi%2Fv1%2Fcompliant_public_repositories%2Fhmpps-digital-prison-reporting-glue-poc)](https://operations-engineering-reports.cloud-platform.service.justice.gov.uk/public-github-repositories.html#hmpps-digital-prison-reporting-glue-poc "Link to report")

#### CODEOWNER

- Team : [hmpps-digital-prison-reporting](https://github.com/orgs/ministryofjustice/teams/hmpps-digital-prison-reporting)
- Email : digitalprisonreporting@digital.justice.gov.uk
- Slack : [#hmpps-dpr-poc](https://mojdt.slack.com/archives/C03TBLUL45B)

**_This repository is for POC only_**

**_Under no circumstance should this repo be hmpps-digital-prison-reporting-glue-poc be considered for prod environment_**

#### ToDo

- [ ] Github Action
- [ ] Deployment Strategy
- [ ] dependabot.yml file
- [ ] refactor code
- [ ] change before and after fields in parquet to string (avoid large complex field)
- [x] acquire s3 bucket/key from glue catalog
- [ ] perhaps combine into single python script
- [ ] Add Kinesis TX event
- [ ] Change to run as micro batch
- [ ] exception capture and reporting
- [ ] update schema in glue
- [ ] retrieve schema from glue (currently acquired from target objects)
- [ ] refresh test data to reflect real world data

## Details

scripts in src prefixed \_ are for dev and generating dummy records

gg_logs_to_parquet - converts goldengate logs to parquet
apply_change_log_to_delta - applies changes in log to delta tables

#### Confluence Page:

[DPR Glue Process](https://dsdmoj.atlassian.net/wiki/spaces/DPR/pages/4166385725/Glue+ETL)

#### Overview

```
TBC
```

#### Further Investigation

```
Reading of delta tables by reporting tools may be problematic
Large number of small files not efficient
Large number of generations in delta versions not efficient
Ingestion of kenesis stream via dynamic frame hasnt been proven
Glue catalogue does not support delta lake
Latency due to rewriting entire versions of datasets in order to upsert and delete

```

#### Code Changes

- Please keep all Code Commentary and Documentation up to date
- Even with the seemingly self-evident the why! is as important as the what and how

#### Branch Naming

- Please use wherever possible the JIRA ticket as branch name.

#### Commits

- Please reference or link any relevant JIRA tickets in commit message.

#### Pull Request

- Please reference or link any relevant JIRA tickets in pull request notes.

#### Local Development or Execution

- Clone this repo
- install requirements

```buildoutcfg
make install
```

## Running in Local Docker

Aquire glue docker image.

```buildoutcfg
docker pull amazon/aws-glue-libs:glue_libs_3.0.0_image_01
```

Export AWS profile as AWS_PROFILE and JUPYTER_WORKSPACE_LOCATION

```buildoutcfg
export AWS_PROFILE=771283872747_modernisation-platform-developer
export JUPYTER_WORKSPACE_LOCATION=~/jupyter_workspace/
```

Run Docker image

```buildoutcfg
docker run -it -v ~/.aws:/home/glue_user/.aws -v $JUPYTER_WORKSPACE_LOCATION:/home/glue_user/workspace/jupyter_workspace/ -e AWS_PROFILE=$AWS_PROFILE -e AWS_REGION=eu-west-2 -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 -p 8998:8998 -p 8888:8888 --name glue_jupyter_lab amazon/aws-glue-libs:glue_libs_3.0.0_image_01 /home/glue_user/jupyter/jupyter_start.sh
```

JupyterLab will be available at

```buildoutcfg
http://localhost:8888/lab
```

###Run Script with delta support
Download required jar from https://repo1.maven.org/maven2/io/delta/

copy delta jar to ~/jupyter_workspace/lib/delta-core_2.12-0.8.0.jar
copy scripts to ~/jupyter_workspace/src/

```buildoutcfg
docker run -it -v ~/.aws:/home/glue_user/.aws -v $JUPYTER_WORKSPACE_LOCATION:/home/glue_user/workspace/ -e AWS_PROFILE=$AWS_PROFILE -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 --name glue_spark_submit amazon/aws-glue-libs:glue_libs_3.0.0_image_01 spark-submit --jars /home/glue_user/workspace/lib/delta-core_2.12-0.8.0.jar --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore  /home/glue_user/workspace/src/<scrpt_name>
```

or use shell scripts \_\_run_local.sh

## Unit Testing and Code Formatting

Code formatting is performed with the command

```buildoutcfg
make black
```

Tests and code format checks are performed against spark with the command

```buildoutcfg
make test
```

### Notes

- Modify the Dependabot file to suit the [dependency manager](https://docs.github.com/en/code-security/dependabot/dependabot-version-updates/configuration-options-for-the-dependabot.yml-file#package-ecosystem) you plan to use and for [automated pull requests for package updates](https://docs.github.com/en/code-security/supply-chain-security/keeping-your-dependencies-updated-automatically/enabling-and-disabling-dependabot-version-updates#enabling-dependabot-version-updates). Dependabot is enabled in the settings by default.

- Ensure as many of the [GitHub Standards](https://github.com/ministryofjustice/github-repository-standards) rules are maintained as possibly can.
