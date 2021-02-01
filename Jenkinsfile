buildMvn {
  publishModDescriptor = 'yes'
  mvnDeploy = 'yes'
  publishAPI = 'yes'
  runLintRamlCop = 'yes'
  buildNode = 'jenkins-agent-java11'

  doDocker = {
    buildJavaDocker {
      publishMaster = 'yes'
      healthChk = 'no'
    }
  }
}
