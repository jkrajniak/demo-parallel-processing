.PHONY: build deploy

STAGE?=dev
REGION?=eu-west-1

EXECUTABLES=$(shell ls cmd)

$(EXECUTABLES):
	CGO_ENABLED=0 GOOS=linux go build -installsuffix nocgo -ldflags="-s -w" -o build/$@ cmd/$@/main.go

build: $(EXECUTABLES)

sls: build
	AWS_SDK_LOAD_CONFIG=1 npx sls deploy --force \
		--stage $(STAGE) \
		--region $(REGION)
