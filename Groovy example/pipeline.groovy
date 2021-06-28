def RUN_DATE = new Date().format("yyyyMMdd")
def PROJECT_ID = "analytics-ahs-owned-thd"
def GIT_REPO = "https://github.homedepot.com/HomeServicesAnalytics/hde-falloff"
def GIT_BRANCH = params.GIT_BRANCH_PARAM
def CONFIG_FILE = "config.ini"
def NOTIFY_SLACK_CHANNEL = "hs_analytics_alerts_dev"
def SLACK_NOTIFY_MEMBERID1 = "UPX4JC0NM" // Brent Brewington

// def NOTEBOOK_HTML_OUTPUT = "hde-falloff/hde_falloff_create_excel_report_${RUN_DATE}.html"
def JENKINS_JOB_NAME = "hde-falloff"
// def CLUSTER = "jupyterlab"
// def OUTPUT_BUCKET = "analytics-ahs-owned-thd-temp"

def COLOR_RED = "FF0000"

def notifyBuild(buildStatus, channel, color, jenkins_job_name, slack_member1) {
  def time = System.currentTimeMillis() / 1000
  def message =  """[{
      "fallback": "<${env.BUILD_URL}|${env.JOB_NAME} [${env.BUILD_NUMBER}]>",
      "color": "${color}",
      "pretext": "${jenkins_job_name} ${buildStatus}",
      "title": "Jenkins Pipeline Link",
      "title_link": "",
      "text": "<${env.BUILD_URL}|${env.JOB_NAME} [${env.BUILD_NUMBER}]> <@${slack_member1}>",
      "footer": "Slack API",
      "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png",
      "ts": ${time}
  }]"""

  slackSend(channel: channel, attachments: message)
}

node {
  stage('Run Queries') {
    try {
      build job: "big_query_runner", parameters: [
        [$class: "StringParameterValue", name: "CONFIG_JSON", value: "sql/steps.json"],
        [$class: "StringParameterValue", name: "CONFIG_PROP", value: CONFIG_FILE],
        [$class: "StringParameterValue", name: "SQL_PROP", value: CONFIG_FILE],
        [$class: "StringParameterValue", name: "GIT_BRANCH", value: "${GIT_BRANCH}"],
        [$class: "StringParameterValue", name: "GIT_REPO", value: "${GIT_REPO}"]
      ]
    } catch(e) {
      currentBuild.result = "FAILURE"
      notifyBuild("FAILED (stage: Run Queries)", NOTIFY_SLACK_CHANNEL, COLOR_RED, JENKINS_JOB_NAME, SLACK_NOTIFY_MEMBERID1)
      throw e
    }
  }
  // stage('Create Excel Report') {
  //     def NOTEBOOK_PARAMS_SPACE_DELIM = [
  //       "run_date=${RUN_DATE}",
  //       "output_bucket=${OUTPUT_BUCKET}"
  //     ].join(" ")
  //
  //     try {
  //       build (job: "data_loads/ami/notebook-runner", parameters: [
  //         [$class: "StringParameterValue", name: "GIT_REPO",         value: "${GIT_REPO}"],
  //         [$class: "StringParameterValue", name: "GIT_BRANCH",       value: "${params.GIT_BRANCH_PARAM}"],
  //         [$class: "StringParameterValue", name: "JOB_NAME",         value: JENKINS_JOB_NAME],
  //         [$class: "StringParameterValue", name: "PROJECT_ID",       value: PROJECT_ID],
  //         [$class: "StringParameterValue", name: "CLUSTER",          value: CLUSTER],
  //         [$class: "StringParameterValue", name: "NOTEBOOK",         value: "create-excel-hde-falloff-report.ipynb"],
  //         [$class: "StringParameterValue", name: "HTML_BUCKET",      value: OUTPUT_BUCKET],
  //         [$class: "StringParameterValue", name: "HTML_BUCKET_PATH", value: NOTEBOOK_HTML_OUTPUT],
  //         [$class: "StringParameterValue", name: "NOTEBOOK_PARAMS",  value: NOTEBOOK_PARAMS_SPACE_DELIM],
  //         [$class: "StringParameterValue", name: "REGION",           value: "us-east1-b"],          // use as default: us-east1-b
  //         [$class: "StringParameterValue", name: "TOLERATIONS",      value: "notebook-n1-std-16"],  // use as default: notebook-n1-std-16
  //         [$class: "StringParameterValue", name: "MEMORY",           value: "20G"],                 // use as default: 20G
  //         [$class: "StringParameterValue", name: "CPU",              value: "6"]                    // use as default: 6
  //               ])
  //     } catch(e) {
  //       currentBuild.result = "FAILURE"
  //       notifyBuild("FAILED (stage: notebook_runner)", NOTIFY_SLACK_CHANNEL, COLOR_RED, JENKINS_JOB_NAME, SLACK_MEMBER_ID1)
  //     }
  //   }
//  stage('Tableau Refresh') {
//    try {
//      build job: "data_loads/tableau_refresh/"
//    } catch(e) {
//      currentBuild.result = "FAILURE"
//      notifyBuild("FAILED (stage: Tableau Refresh)", NOTIFY_SLACK_CHANNEL, "FF0000")
//    }
//  }
}
