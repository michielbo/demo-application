.PHONY: build clean

NAME := "app"
VERSION := $(shell python3 setup.py -V)
RPMDIR := "$(shell pwd)/rpms"

default: build

build:
	python3 setup.py sdist

clean:
	rm -rf build dist *.egg-info rpms

rpm: build
	rm -rf rpms/*
	mock -r epel-7-x86_64 -D "pyversion ${VERSION}" --buildsrpm --spec ${NAME}.spec --sources dist --resultdir rpms
	mock -r epel-7-x86_64 -D "pyversion ${VERSION}" --rebuild rpms/${NAME}-*.src.rpm --resultdir rpms

upload: RPM := $(shell basename ${RPMDIR}/${NAME}-${VERSION}-*.x86_64.rpm)

upload:
	@echo Uploading $(RPM)
	ssh repomanager@jenkins.dmz.ii.inmanta.com "/usr/bin/repomanager --config /etc/repomanager.toml --repo training --distro el7 --file - --file-name ${RPM}" < ${RPMDIR}/${RPM}
