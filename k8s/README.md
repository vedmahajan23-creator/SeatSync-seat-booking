# Local Kubernetes deployment

Run these commands from the project root in Windows PowerShell. The frontend
serves `frontend\index.html` and `frontend\labs.html` from a ConfigMap using the
stock `nginx:alpine` image.

## Build and load images

Choose one local cluster workflow.

### Minikube

```powershell
minikube start
minikube image build -t seatsync-venue:latest -f venue/Dockerfile .
minikube image build -t seatsync-coordinator:latest -f coordinator/Dockerfile .
```

### kind

```powershell
kind create cluster
docker build -t seatsync-venue:latest -f venue/Dockerfile .
docker build -t seatsync-coordinator:latest -f coordinator/Dockerfile .
kind load docker-image seatsync-venue:latest seatsync-coordinator:latest
```

## Deploy

```powershell
kubectl create configmap seatsync-frontend --from-file=index.html=frontend/index.html --from-file=labs.html=frontend/labs.html --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s
kubectl rollout status deployment/mongodb
kubectl rollout status deployment/venue-a
kubectl rollout status deployment/venue-b
kubectl rollout status deployment/coordinator-1
kubectl rollout status deployment/coordinator-2
kubectl rollout status deployment/frontend
```

The frontend Service exposes port `8080` and NodePort `30080`. For a consistent
local URL with either cluster, run this in a separate PowerShell window:

```powershell
kubectl port-forward service/frontend 8080:8080
```

The existing frontend calls coordinator URLs on `127.0.0.1:5000` and
`127.0.0.1:5010`. Run these in two more PowerShell windows so those browser
requests reach the coordinator Services:

```powershell
kubectl port-forward service/coordinator-1 5000:5000
```

```powershell
kubectl port-forward service/coordinator-2 5010:5010
```

Then open <http://localhost:8080>. With Minikube, you can also use
`minikube service frontend --url` to access its NodePort, while keeping the
coordinator port-forwards running.

## Cleanup

```powershell
kubectl delete -f k8s
kubectl delete configmap seatsync-frontend
```

The MongoDB PVC is included in cleanup; depending on the cluster's storage
provisioner and reclaim policy, its backing data may also be deleted. To remove
the local cluster afterward, run `minikube delete` or `kind delete cluster`.
