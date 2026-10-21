IMAGE=quay.io/bjoydeep/change-sentinel
IMAGE_OPERATOR=quay.io/bjoydeep/widget-operator
TAG?=latest
VENV=.venv

# ── Local development ─────────────────────────────────────────────────────────

venv:
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -r sentinel/requirements.txt -r demo-operator/requirements.txt
	@echo "Run: source $(VENV)/bin/activate"

run-local:
	cd sentinel && ../.venv/bin/python main.py

run-operator:
	cd demo-operator && ../.venv/bin/kopf run operator.py --namespace demo-operator-ns

test:
	$(VENV)/bin/pytest sentinel/tests/ -v

# ── Container ─────────────────────────────────────────────────────────────────

build:
	podman build --platform linux/amd64 -t $(IMAGE):$(TAG) sentinel/

push:
	podman push $(IMAGE):$(TAG)

build-push: build push

build-operator:
	podman build --platform linux/amd64 -t $(IMAGE_OPERATOR):$(TAG) demo-operator/

push-operator:
	podman push $(IMAGE_OPERATOR):$(TAG)

build-push-operator: build-operator push-operator

run-container:
	podman run \
		--env-file .env \
		-v $(HOME)/.kube:/home/sentinel/.kube:ro \
		-p 8000:8000 \
		$(IMAGE):$(TAG)

# ── Cluster deployment ────────────────────────────────────────────────────────

deploy-demo-operator:
	kubectl apply -f demo-operator/widget-crd.yaml
	kubectl create namespace demo-operator-ns --dry-run=client -o yaml | kubectl apply -f -

deploy-operator: deploy-demo-operator
	kubectl apply -f demo-operator/deploy/rbac.yaml
	kubectl apply -f demo-operator/deploy/deployment.yaml
	@echo ""
	@echo "Waiting for operator rollout..."
	kubectl rollout status deployment/widget-operator -n demo-operator-ns

undeploy-operator:
	kubectl delete -f demo-operator/deploy/deployment.yaml --ignore-not-found
	kubectl delete -f demo-operator/deploy/rbac.yaml       --ignore-not-found

deploy: deploy-demo-operator
	kubectl apply -f deploy/namespace.yaml
	kubectl apply -f deploy/rbac.yaml
	kubectl apply -f deploy/configmap.yaml
	kubectl apply -f deploy/secret.yaml
	kubectl apply -f deploy/pvc.yaml
	kubectl apply -f deploy/deployment.yaml
	kubectl apply -f deploy/service.yaml
	@echo ""
	@echo "Waiting for rollout..."
	kubectl rollout status deployment/change-sentinel -n change-sentinel

undeploy:
	kubectl delete -f deploy/service.yaml     --ignore-not-found
	kubectl delete -f deploy/deployment.yaml  --ignore-not-found
	kubectl delete -f deploy/pvc.yaml         --ignore-not-found
	kubectl delete -f deploy/secret.yaml      --ignore-not-found
	kubectl delete -f deploy/configmap.yaml   --ignore-not-found
	kubectl delete -f deploy/rbac.yaml        --ignore-not-found
	kubectl delete -f deploy/namespace.yaml   --ignore-not-found

# ── Observability ─────────────────────────────────────────────────────────────

logs:
	kubectl logs -n change-sentinel -l app=change-sentinel -f

health:
	kubectl port-forward -n change-sentinel svc/change-sentinel 8000:8000 &
	@sleep 1
	curl -s http://localhost:8000/api/v1/health?clusters=my-cluster | python3 -m json.tool

# ── Helpers ───────────────────────────────────────────────────────────────────

secret:
	@echo "Copying secret.template → deploy/secret.yaml"
	cp deploy/secret.template deploy/secret.yaml
	@echo "Edit deploy/secret.yaml with real values, then: kubectl apply -f deploy/secret.yaml"

.PHONY: venv run-local run-operator test \
        build push build-push run-container \
        build-operator push-operator build-push-operator \
        deploy-demo-operator deploy-operator undeploy-operator deploy undeploy \
        logs health secret
