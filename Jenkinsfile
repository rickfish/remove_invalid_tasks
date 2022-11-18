#!/usr/bin/env groovy
@Library('cplib@release/2.0.0') _
import com.bcbsfl.paas.cplib.helpers.Build
import com.bcbsfl.paas.cplib.helpers.Workspace
import com.bcbsfl.paas.cplib.utils.Openshift
 
def bld = new Build()
def wspace = new Workspace()   
def osh = new Openshift()
def gradleTasks = "clean compileJava build"
def projectName = "netflix-conductor"
def appName = "conductor-remove-invalid-tasks"
def checkoutAppOcpConfigBranch  = "master"
def appReleaseTag = "${BUILD_NUMBER}"



pipeline {

	agent any
	
	options {
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '30', daysToKeepStr: '15'))
        skipDefaultCheckout(true)
        timestamps()
        skipStagesAfterUnstable()
    }

   parameters {
        string (
            name : 'RELEASE_VERSION',
            defaultValue: '',
            description: '[Optional] Application Docker release tag (e.g. master, 1.0.0, 1.0.0.15). Please use this if you need to deploy an earlier version of the code that is already built (No build will be done in this case).'
        )
 		booleanParam (
            name: 'UNIT_DEPLOY',
            defaultValue: true,
            description: 'Please check this if you want to deploy to the Unit (netflix-conductor-unit namespace) environment.'
        )
		booleanParam (
            name: 'TEST_DEPLOY',
            defaultValue: false,
            description: 'Please check this if you want to deploy to the Test (netflix-conductor-test namespace) environment.'
        )
		booleanParam (
            name: 'STAGE_DEPLOY',
            defaultValue: false,
            description: 'Please check this if you want to deploy to the Stage (netflix-conductor--stage namespace) environment.'
        )
        booleanParam(
        	name: 'PROD',
        	defaultValue: false, 
        	description: 'Boolean Flag for PROD deploy: [true] - Run PROD stages [false] - Skip PROD stages' 
        )
        string(
        	name: 'NOTIFICATION',
        	defaultValue: 'some.one@bcbsfl.com', 
        	description: 'Comma-delimited list of recipients to receive Approval Notifications' 
        )
        string(
        	name: 'TICKET',
        	defaultValue: '', 
        	description: 'Remedy Task (TAS)' 
        )
    }

    stages {  
		stage('Prepare') {
			steps {
				script {
					log.info "******************* AFTER BUILD, DEPLOYING TO THESE ENVIRONMENTS *********************"
					if(params.UNIT_DEPLOY == true) {
						log.info("Unit")
					}
					if(params.TEST_DEPLOY == true) {
						log.info("Test")
					}
					if(params.STAGE_DEPLOY == true) {
						log.info("Stage")
					}
					if(params.PROD == true) {
						log.info("Prod")
					}
					log.info "**************************************************************************************"
					log.info "Explicit scm checkout ..."
	       			checkout scm
	                wspace.init()
//	                wspace.checkoutAppOcpConfig(appName, checkoutAppOcpConfigBranch)
                    log.info "after checkout..."
                    appReleaseTag = wspace.getBuildProperty("version") + "." + env.BUILD_NUMBER
				}
			}
		}

        stage('Build Artifacts') {
            steps {
                script {
	                 bld.gradle("gradle-5.2.1", gradleTasks + " --refresh-dependencies")
                    log.info "Preparing Image contents ..."
                    tmp_dir = "tmp"
                    deployments_dir = tmp_dir
                    log.info "Copy necessary build artifacts into the deployments dir ..."
                    sh """
	                    ls -lt .
	                    ls -lt ./build
                        mkdir -p ${deployments_dir}
                        find build -name "*.jar" -exec sh -c 'cp "\$@" "\$0"' ${deployments_dir} {} +
                        mv ${deployments_dir}/*.jar ${deployments_dir}/${appName}.jar
                        chmod -R 777 ${tmp_dir}

                        # Archive Build
                        tar -cjf ${appName}.tar -C ${tmp_dir} .
                    """
                    log.info "Displaying artifacts ..."
                    sh "ls -lt ${deployments_dir}"
                }
            }
	    }
	    stage('Build Image') {
            steps {
                script {
                    if(!wspace.releaseVersionExists()) {
						osh.oseLoginDev(constantsutil.FLBLUE_OSE4X_APPS)
                        appReleaseTag = osh.buildAppImage(appName, appReleaseTag)
						osh.oseLogout()
                    } else {
                        appReleaseTag = env.RELEASE_VERSION
                    }
                    log.info "Displaying artifacts ..."
                    sh "ls -lt ${deployments_dir}"
                }
            }
	    }//Close stage 'Build Image'

	    stage('Deploy Unit') {
            steps {
                script {
	               	if(params.UNIT_DEPLOY == true) {
					    envName="unit"
						osh.oseLoginDev(constantsutil.FLBLUE_OSE4X_APPS)
						osh.deploy(projectName+"-"+envName, appName, envName, appReleaseTag)
						osh.oseLogout()
					} else {
						log.info "Unit deploy not checked. Not deploying to Unit."
					}
				}
            }
        }//Close stage 'Deploy Unit'

	    stage('Deploy Test') {
            steps {
                script {
	               	if(params.TEST_DEPLOY == true) {
					    envName="test"
						osh.oseLoginDev(constantsutil.FLBLUE_OSE4X_APPS)
						osh.deploy(projectName+"-"+envName, appName, envName, appReleaseTag)
						osh.oseLogout()
					} else {
						log.info "Test deploy not checked. Not deploying to Test."
					}
                }
            }
        }//Close stage 'Deploy Test'

	    stage('Deploy Stage') {
            steps {
                script {
	               	if(params.STAGE_DEPLOY == true) {
					    envName="stage"
						osh.oseLoginDev(constantsutil.FLBLUE_OSE4X_APPS)
						osh.deploy(projectName+"-"+envName, appName, envName, appReleaseTag)
						osh.oseLogout()
					} else {
						log.info "Stage deploy not checked. Not deploying to Stage."
					}
                }
            }
        }//Close stage 'Deploy Stage'
        
        
       stage('Approval: Prod') {
            agent none
            steps {
                echo "Approval for Prod..."
                checkRemedyStatus prod: "${params.PROD}", notification: "${params.NOTIFICATION}", ticket: "${params.TICKET}"
            }
        }
        

        stage('Deploy Prod') {
            steps {
                script {
	               	if(params.PROD == true) {
					    envName="prod"
						osh.oseLoginProd(constantsutil.FLBLUE_OSE4X_APPS)
						osh.promoteAppImage(appName, appReleaseTag, 
							constantsutil.FLBLUE_OSE4X_APPS.dockerDevhost, "etd-image",
							constantsutil.FLBLUE_OSE4X_APPS.dockerProdhost, "etd-image")
						osh.deploy(projectName+"-"+envName, appName, envName, appReleaseTag)
						osh.oseLogout()
					} else {
						log.info "Prod deploy not checked. Not deploying to Prod."
					}
                }
            }
        }//Close stage 'Deploy Prod'

        stage('Baseline code for Prod') {
            steps {
                script {
	               	if(params.PROD == true) {
						wspace.tagAppCodeAndConfigRepo(appReleaseTag)
					} else {
						log.info "Baselining not done. Not deploying to Prod."
					}
                }
            }
        }//Close stage 'Baseline code for Prod'
	}
}
