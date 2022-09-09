# Ministry of Justice Digital Prison Reporting Glue Scripts

[![repo standards badge](https://img.shields.io/badge/dynamic/json?color=blue&style=for-the-badge&logo=github&label=MoJ%20Compliant&query=%24.data%5B%3F%28%40.name%20%3D%3D%20%22hmpps-digital-prison-reporting-glue-poc%22%29%5D.status&url=https%3A%2F%2Foperations-engineering-reports.cloud-platform.service.justice.gov.uk%2Fgithub_repositories)](https://operations-engineering-reports.cloud-platform.service.justice.gov.uk/github_repositories#hmpps-digital-prison-reporting-glue-poc "Link to report")

#### CODEOWNER

- Team : [hmpps-digital-prison-reporting](https://github.com/orgs/ministryofjustice/teams/hmpps-digital-prison-reporting)
- Email : digitalprisonreporting@digital.justice.gov.uk
- Slack : [#hmpps-dpr-poc](https://mojdt.slack.com/archives/C03TBLUL45B)

**_This repository is for POC only_**

**_Under no circumstance should this repo be hmpps-digital-prison-reporting-glue-poc be considered for prod environment_**

#### ToDo

- [ ] Github Action example
- [ ] dependabot.yml file
- [x] CODEOWNERS
- [ ] refactor code

## Details

#### Confluence Page:

[TBC]()

#### Overview

```
TBC
```

#### Branch Naming

- Please use wherever possible the JIRA ticket as branch name.

#### Commits

- Please reference or link any relevant JIRA tickets in commit message.

#### Pull Request

- Please reference or link any relevant JIRA tickets in pull request notes.

#### Local Development or Execution

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

### Notes

- Modify the Dependabot file to suit the [dependency manager](https://docs.github.com/en/code-security/dependabot/dependabot-version-updates/configuration-options-for-the-dependabot.yml-file#package-ecosystem) you plan to use and for [automated pull requests for package updates](https://docs.github.com/en/code-security/supply-chain-security/keeping-your-dependencies-updated-automatically/enabling-and-disabling-dependabot-version-updates#enabling-dependabot-version-updates). Dependabot is enabled in the settings by default.

- Ensure as many of the [GitHub Standards](https://github.com/ministryofjustice/github-repository-standards) rules are maintained as possibly can.
