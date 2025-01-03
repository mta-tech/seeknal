.PHONY: clean default virtualenv doc test integ-test build publish

default: clean doc build

VERSION_NUM := $(shell ./gradlew -q printVersion)
SPARK_VERSION := 3.5.4
PACKAGE_NAME := spark-engine-spark${SPARK_VERSION}-${VERSION_NUM}
IMAGE_NAME := eurekaai/spark:2.2.1-hadoop2.7-python2.7.15-modeng

# determine the current virtualenv
virtualenv:
ifeq ($(origin VIRTUAL_ENV), undefined)
    venv := .venv
else
    venv := $(VIRTUAL_ENV)
endif

buildNumber:
ifeq ($(origin BUILD_NUMBER), undefined)
   BUILD_NUMBER := SNAPSHOT
endif

# create virtualenv if not exists
${venv}: requirements-doc.txt
	test -d ${venv} || virtualenv ${venv}

copy-requirements:
	${venv}/bin/pip install -r requirements-copy.txt

clean:
	rm -rf build/ doc/_build/ dist/ metastore_db/ spark-warehouse/ pom.xml

test: print-spark-version
	# ./gradlew scalaStyle -PsparkVersion=${SPARK_VERSION} test
	./gradlew -PsparkVersion=${SPARK_VERSION} test

test-coverage:
	@echo "Checking coverage test criteria (min: 80%) by running/analyzing tests, if passes => generates reports"
	./gradlew clean checkScoverage

print-spark-version:
	@echo "Spark Version: ${SPARK_VERSION}"

print-version:
	@echo ${VERSION_NUM}-${SPARK_VERSION}

print-build-number: buildNumber
	@echo ${BUILD_NUMBER}

createPom:
	./gradlew -PsparkVersion=${SPARK_VERSION} createPom

copyIgniteConfig:
	./gradlew -PigniteHostOne=${IGNITE_HOST_ONE} -PigniteHostTwo=${IGNITE_HOST_TWO} copyIgniteConfig

build: print-spark-version
	./gradlew -PsparkVersion=${SPARK_VERSION} shadowJar

publish:
	./gradlew -PsparkVersion=${SPARK_VERSION} publish

publish-fat:
	./gradlew -PsparkVersion=${SPARK_VERSION} -p fat-build publish

docker-package:
	docker run --rm -v ${PWD}:/code -ti -w /code \
		-e VIRTUAL_ENV=/env \
		-e GRADLE_OPTS=-Dgradle.user.home=/code/.gradle \
		${IMAGE_NAME} make clean package

docker-interactive:
	docker run --rm -v ${PWD}:/code -ti -w /code \
		-e VIRTUAL_ENV=/env \
		-e GRADLE_OPTS=-Dgradle.user.home=/code/.gradle \
		${IMAGE_NAME} /bin/sh

package: build buildNumber
	mkdir -p dist/${PACKAGE_NAME}-${BUILD_NUMBER}/lib/
	mkdir -p dist/${PACKAGE_NAME}-${BUILD_NUMBER}/playbook/
	cp -r bin dist/${PACKAGE_NAME}-${BUILD_NUMBER}/
	cp -r conf dist/${PACKAGE_NAME}-${BUILD_NUMBER}/
	cp -r packages dist/${PACKAGE_NAME}-${BUILD_NUMBER}/
	cp build/libs/spark-engine-${VERSION_NUM}-all.jar dist/${PACKAGE_NAME}-${BUILD_NUMBER}/lib/spark-engine.jar
	make print-build-number > dist/${PACKAGE_NAME}-${BUILD_NUMBER}/BUILD_NUMBER
	make print-version > dist/${PACKAGE_NAME}-${BUILD_NUMBER}/VERSION
	cd dist/ && tar czf ${PACKAGE_NAME}-${BUILD_NUMBER}.tar.gz ${PACKAGE_NAME}-${BUILD_NUMBER}
	rm -r dist/${PACKAGE_NAME}-${BUILD_NUMBER}

add-changelog:
	git-chglog -o CHANGELOG.md
