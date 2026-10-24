.PHONY: build run test vet tidy docker clean

build:
	./scripts/build.sh

run:
	./scripts/run-dev.sh

test:
	./scripts/test.sh

vet:
	go vet ./...

tidy:
	go mod tidy

docker:
	docker build -f deployments/docker/Dockerfile -t caimandb:local .

clean:
	rm -rf bin data-dev
