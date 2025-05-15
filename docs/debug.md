# Helpful Commands for Debugging

```sh
kubectl run dns-test --rm -it --image=busybox:latest -n <your-namespace> -- sh
```

# List processes on pod
```sh
kubectl exec -it <pod-name> -- sh -c "ls -la /proc/[0-9]*/cmdline | awk -F/ '{print \$3}' | xargs -I{} sh -c 'echo -n \"PID {} - \"; cat /proc/{}/cmdline | tr \"\\0\" \" \"; echo'"
```

# Check number of failover events in MongoDB
```sh
db.proxy_logs.find({ event: "failover" }).count()
```