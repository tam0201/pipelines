IMG ?= asia.gcr.io/om-hkt21-team8/kubeflow/metadata-writer
TAG ?= 1.0
DOCKERFILE=metadata_writer/Dockerfile

docker-build:
	cp Dockerfile.dockerignore ../.dockerignore
	-cd ../ && docker build -t ${IMG}:${TAG} -f ${DOCKERFILE} .
	rm ../.dockerignore


docker-push:
	docker push $(IMG):$(TAG)

image: docker-build docker-push
	@echo "Updated image ${IMG}"