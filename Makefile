ROOT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
DOCKER_IMG=josefine-builder
DOCKER_CMD=docker run --rm -ti --name=josefine-builder -v $(DOCKER_IMG):/root/.cargo -v $(ROOT_DIR):/opt/josefine $(DOCKER_IMG)

build-image :
	docker volume create $(DOCKER_IMG)
	docker build . --tag $(DOCKER_IMG):latest

build :
	$(DOCKER_CMD) build

test :
	$(DOCKER_CMD) test
