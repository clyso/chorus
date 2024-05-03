# chorus

![Version: 0.1.1](https://img.shields.io/badge/Version-0.1.1-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: v0.5.2](https://img.shields.io/badge/AppVersion-v0.5.2-informational?style=flat-square)

Helm chart for Chorus S3 management software.

**Homepage:** <https://github.com/clyso/chorus>

## Requirements

| Repository | Name | Version |
|------------|------|---------|
| https://charts.bitnami.com/bitnami | redis | 17.11.3 |

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| agent.agentService.portHttp.port | int | `9673` |  |
| agent.agentService.portHttp.targetPort | string | `"http"` |  |
| agent.agentService.type | string | `"ClusterIP"` |  |
| agent.config.fromStorage | string | `""` |  |
| agent.config.log.json | bool | `true` |  |
| agent.config.log.level | string | `"info"` |  |
| agent.config.metrics.enabled | bool | `false` |  |
| agent.config.port | int | `9673` |  |
| agent.config.trace.enabled | bool | `false` |  |
| agent.config.trace.endpoint | string | `nil` |  |
| agent.config.url | string | `"http://localhost:9673"` |  |
| agent.enabled | bool | `false` |  |
| agent.image.pullPolicy | string | `"Always"` |  |
| agent.image.repository | string | `"harbor.clyso.com/chorus/agent"` |  |
| agent.image.tag | string | `""` |  |
| agent.replicas | int | `1` |  |
| agent.resources.limits.cpu | string | `"100m"` |  |
| agent.resources.limits.memory | string | `"128Mi"` |  |
| agent.resources.requests.cpu | string | `"50m"` |  |
| agent.resources.requests.memory | string | `"64Mi"` |  |
| commonConfig.features.acl | bool | `true` |  |
| commonConfig.features.tagging | bool | `true` |  |
| commonConfig.storage.createReplication | bool | `false` |  |
| commonConfig.storage.createRouting | bool | `true` |  |
| commonConfig.storage.storages.one.address | string | `"s3.clyso.com"` |  |
| commonConfig.storage.storages.one.isMain | bool | `true` |  |
| commonConfig.storage.storages.one.isSecure | bool | `true` |  |
| commonConfig.storage.storages.one.provider | string | `"Ceph"` |  |
| commonConfig.storage.storages.two.address | string | `"office.clyso.cloud"` |  |
| commonConfig.storage.storages.two.isSecure | bool | `true` |  |
| commonConfig.storage.storages.two.provider | string | `"Ceph"` |  |
| existingSecret | string | `""` |  |
| imagePullSecrets | string | `nil` |  |
| proxy.config.address | string | `"http://localhost:9669"` |  |
| proxy.config.auth.allowV2Signature | bool | `false` |  |
| proxy.config.auth.custom | string | `nil` |  |
| proxy.config.auth.useStorage | string | `nil` |  |
| proxy.config.cors.allowAll | bool | `false` |  |
| proxy.config.cors.enabled | bool | `false` |  |
| proxy.config.cors.whitelist | string | `nil` |  |
| proxy.config.log.json | bool | `true` |  |
| proxy.config.log.level | string | `"info"` |  |
| proxy.config.metrics.enabled | bool | `false` |  |
| proxy.config.port | int | `9669` |  |
| proxy.config.trace.enabled | bool | `false` |  |
| proxy.config.trace.endpoint | string | `nil` |  |
| proxy.enabled | bool | `true` |  |
| proxy.image.pullPolicy | string | `"Always"` |  |
| proxy.image.repository | string | `"harbor.clyso.com/chorus/proxy"` |  |
| proxy.image.tag | string | `""` |  |
| proxy.proxyService.portHttp.port | int | `9669` |  |
| proxy.proxyService.portHttp.targetPort | string | `"http"` |  |
| proxy.proxyService.type | string | `"ClusterIP"` |  |
| proxy.replicas | int | `2` |  |
| proxy.resources.limits.cpu | string | `"100m"` |  |
| proxy.resources.limits.memory | string | `"300Mi"` |  |
| proxy.resources.requests.cpu | string | `"50m"` |  |
| proxy.resources.requests.memory | string | `"150Mi"` |  |
| redis.architecture | string | `"standalone"` |  |
| redis.auth.existingSecret | string | `"redis-secret"` |  |
| redis.auth.existingSecretPasswordKey | string | `"password"` |  |
| redis.enabled | bool | `true` |  |
| secret | string | `""` |  |
| serviceAccount.annotations | object | `{}` |  |
| serviceAccount.create | bool | `true` |  |
| serviceAccount.name | string | `""` |  |
| worker.config.api.enabled | bool | `true` |  |
| worker.config.concurrency | int | `10` |  |
| worker.config.log.json | bool | `true` |  |
| worker.config.log.level | string | `"info"` |  |
| worker.config.metrics.enabled | bool | `false` |  |
| worker.config.rclone.globalFileLimit.enabled | bool | `true` |  |
| worker.config.rclone.globalFileLimit.limit | int | `3` |  |
| worker.config.rclone.globalFileLimit.retryMax | string | `"20s"` |  |
| worker.config.rclone.globalFileLimit.retryMin | string | `"2s"` |  |
| worker.config.rclone.localFileLimit.enabled | bool | `false` |  |
| worker.config.rclone.localFileLimit.limit | int | `3` |  |
| worker.config.rclone.localFileLimit.retryMax | string | `"20s"` |  |
| worker.config.rclone.localFileLimit.retryMin | string | `"2s"` |  |
| worker.config.rclone.memoryCalc.const | string | `"0M"` |  |
| worker.config.rclone.memoryCalc.mul | float | `1.5` |  |
| worker.config.rclone.memoryLimit.enabled | bool | `true` |  |
| worker.config.rclone.memoryLimit.limit | string | `"300M"` |  |
| worker.config.rclone.memoryLimit.retryMax | string | `"20s"` |  |
| worker.config.rclone.memoryLimit.retryMin | string | `"2s"` |  |
| worker.config.trace.enabled | bool | `false` |  |
| worker.config.trace.endpoint | string | `nil` |  |
| worker.grpcApiService.portGRPC.port | int | `9670` |  |
| worker.grpcApiService.portGRPC.targetPort | string | `"grpc"` |  |
| worker.grpcApiService.type | string | `"ClusterIP"` |  |
| worker.image.pullPolicy | string | `"Always"` |  |
| worker.image.repository | string | `"harbor.clyso.com/chorus/worker"` |  |
| worker.image.tag | string | `""` |  |
| worker.replicas | int | `2` |  |
| worker.resources.limits.cpu | string | `"100m"` |  |
| worker.resources.limits.memory | string | `"300Mi"` |  |
| worker.resources.requests.cpu | string | `"50m"` |  |
| worker.resources.requests.memory | string | `"150Mi"` |  |
| worker.restApiService.portRest.port | int | `9071` |  |
| worker.restApiService.portRest.targetPort | string | `"rest"` |  |
| worker.restApiService.type | string | `"ClusterIP"` |  |

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.13.1](https://github.com/norwoodj/helm-docs/releases/v1.13.1)
