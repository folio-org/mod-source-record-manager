buildMvn {
  publishModDescriptor = 'yes'
  mvnDeploy = 'yes'
  buildNode = 'jenkins-agent-java17'

  doDocker = {
    buildJavaDocker {
      publishMaster = 'yes'
      healthChk = 'no'
    }
  }
}
