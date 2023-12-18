IMAGE_NAME = transit-scraper
IMAGE_REGISTRY = us.gcr.io/zeitgeistlabs
IMAGE_TAG = $(shell git rev-parse --short HEAD)
KUBERNETES_NAMESPACE = transit-data

build:
	docker build -t $(IMAGE_REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG) .

push: build
	docker push $(IMAGE_REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)

deploy: push
	cat resources.yaml | sed s/TAG/$(IMAGE_TAG)/g | kubectl -n $(KUBERNETES_NAMESPACE) apply -f -

secret:
	kubectl -n $(KUBERNETES_NAMESPACE) create secret generic transit-scraper-config --from-file=cmd/scraper/production.json
