{{/*
Expand the name of the chart.
*/}}
{{- define "chorus.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "chorus.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "chorus.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "chorus.labels" -}}
helm.sh/chart: {{ include "chorus.chart" . }}
{{ include "chorus.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "chorus.selectorLabels" -}}
app.kubernetes.io/name: {{ include "chorus.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "chorus.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "chorus.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Credentials secret name
*/}}
{{- define "chorus.credentialsSecretName" -}}
{{- if .Values.existingCredentialsSecret }}
{{- .Values.existingCredentialsSecret }}
{{- else }}
{{- printf "%s-credentials" (include "chorus.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Redis secret name
*/}}
{{- define "chorus.redisSecretName" -}}
{{- if .Values.redis.enabled }}
{{- if .Values.redis.auth.existingSecret }}
{{- .Values.redis.auth.existingSecret }}
{{- else }}
{{- /* Use Bitnami Redis chart's auto-generated secret */}}
{{- printf "%s-redis" .Release.Name }}
{{- end }}
{{- else }}
{{- if .Values.externalRedis.existingSecret }}
{{- .Values.externalRedis.existingSecret }}
{{- else }}
{{- printf "%s-redis-external" (include "chorus.fullname" .) }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Redis secret key
*/}}
{{- define "chorus.redisSecretKey" -}}
{{- if .Values.redis.enabled }}
{{- .Values.redis.auth.existingSecretPasswordKey | default "redis-password" }}
{{- else }}
{{- .Values.externalRedis.existingSecretPasswordKey | default "password" }}
{{- end }}
{{- end }}

{{/*
Redis addresses
*/}}
{{- define "chorus.redisAddresses" -}}
{{- if .Values.redis.enabled }}
{{- printf "%s-redis-master:6379" .Release.Name }}
{{- else }}
{{- join "," .Values.externalRedis.addresses }}
{{- end }}
{{- end }}

{{/*
Check if credentials secret should be created
*/}}
{{- define "chorus.createCredentialsSecret" -}}
{{- if and (not .Values.existingCredentialsSecret) .Values.credentials.storages }}
true
{{- end }}
{{- end }}

{{/*
Dynamic credentials secret name
*/}}
{{- define "chorus.dynamicCredentialsSecretName" -}}
{{- if .Values.dynamicCredentials.existingSecret }}
{{- .Values.dynamicCredentials.existingSecret }}
{{- else }}
{{- printf "%s-dynamic-credentials" (include "chorus.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Dynamic credentials secret key
*/}}
{{- define "chorus.dynamicCredentialsSecretKey" -}}
{{- .Values.dynamicCredentials.existingSecretKey | default "master-password" }}
{{- end }}

{{/*
Check if dynamic credentials secret should be created
*/}}
{{- define "chorus.createDynamicCredentialsSecret" -}}
{{- if and .Values.dynamicCredentials.enabled (not .Values.dynamicCredentials.existingSecret) .Values.dynamicCredentials.masterPassword }}
true
{{- end }}
{{- end }}

{{/*
Check if redis secret should be created (for external redis with inline password)
*/}}
{{- define "chorus.createRedisSecret" -}}
{{- if and (not .Values.redis.enabled) (not .Values.externalRedis.existingSecret) .Values.externalRedis.password }}
true
{{- end }}
{{- end }}

{{/*
Build storage config for worker (full config including authURL for Swift)
*/}}
{{- define "chorus.workerStorageConfig" -}}
storage:
  {{- if .Values.storage.main }}
  main: {{ .Values.storage.main | quote }}
  {{- end }}
  {{- if .Values.dynamicCredentials.enabled }}
  dynamicCredentials:
    enabled: true
    disableEncryption: {{ .Values.dynamicCredentials.disableEncryption }}
    pollInterval: {{ .Values.dynamicCredentials.pollInterval }}
  {{- end }}
  {{- if .Values.storage.storages }}
  storages:
    {{- range $name, $storage := .Values.storage.storages }}
    {{ $name }}:
      {{- /* Copy all fields except storageURL which is proxy-only */}}
      {{- range $key, $value := $storage }}
      {{- if ne $key "storageURL" }}
      {{ $key }}: {{ $value | toYaml | nindent 6 | trim }}
      {{- end }}
      {{- end }}
    {{- end }}
  {{- end }}
{{- end }}

{{/*
Build storage config for proxy (uses storageURL for Swift, excludes authURL and worker-only fields)
*/}}
{{- define "chorus.proxyStorageConfig" -}}
storage:
  {{- if .Values.storage.main }}
  main: {{ .Values.storage.main | quote }}
  {{- end }}
  {{- if .Values.dynamicCredentials.enabled }}
  dynamicCredentials:
    enabled: true
    disableEncryption: {{ .Values.dynamicCredentials.disableEncryption }}
    pollInterval: {{ .Values.dynamicCredentials.pollInterval }}
  {{- end }}
  {{- if .Values.storage.storages }}
  storages:
    {{- range $name, $storage := .Values.storage.storages }}
    {{ $name }}:
      {{- $isSwift := eq (toString (index $storage "type")) "SWIFT" }}
      {{- range $key, $value := $storage }}
      {{- /* For Swift: use storageURL, skip authURL and Keystone fields */}}
      {{- if $isSwift }}
        {{- if not (or (eq $key "authURL") (eq $key "storageEndpointName") (eq $key "storageEndpointType") (eq $key "region")) }}
      {{ $key }}: {{ $value | toYaml | nindent 6 | trim }}
        {{- end }}
      {{- else }}
        {{- /* For S3: copy all fields except storageURL */}}
        {{- if ne $key "storageURL" }}
      {{ $key }}: {{ $value | toYaml | nindent 6 | trim }}
        {{- end }}
      {{- end }}
      {{- end }}
    {{- end }}
  {{- end }}
{{- end }}

{{/*
Merge node selector with global
*/}}
{{- define "chorus.nodeSelector" -}}
{{- $local := index . 0 }}
{{- $global := index . 1 }}
{{- $merged := merge ($local | default dict) ($global | default dict) }}
{{- if $merged }}
{{- toYaml $merged }}
{{- end }}
{{- end }}

{{/*
Merge tolerations with global
*/}}
{{- define "chorus.tolerations" -}}
{{- $local := index . 0 }}
{{- $global := index . 1 }}
{{- $merged := concat ($local | default list) ($global | default list) }}
{{- if $merged }}
{{- toYaml $merged }}
{{- end }}
{{- end }}

{{/*
Merge affinity with global
*/}}
{{- define "chorus.affinity" -}}
{{- $local := index . 0 }}
{{- $global := index . 1 }}
{{- $merged := merge ($local | default dict) ($global | default dict) }}
{{- if $merged }}
{{- toYaml $merged }}
{{- end }}
{{- end }}
