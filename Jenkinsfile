buildMvn {
  publishModDescriptor = 'yes'
  mvnDeploy = 'yes'
  buildNode = 'jenkins-agent-java11'

  doDocker = {
    buildJavaDocker {
      publishMaster = 'yes'
      healthChk = 'no'
    }
  }
}
