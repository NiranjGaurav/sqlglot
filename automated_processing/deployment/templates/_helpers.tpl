{{/*
Expand the name of the chart.
*/}}
{{- define "sqlglot-backend.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "sqlglot-backend.fullname" -}}
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
{{- define "sqlglot-backend.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "sqlglot-backend.labels" -}}
helm.sh/chart: {{ include "sqlglot-backend.chart" . }}
{{ include "sqlglot-backend.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels for API
*/}}
{{- define "sqlglot-backend.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sqlglot-backend.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
API specific labels
*/}}
{{- define "sqlglot-backend.api.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sqlglot-backend.name" . }}-api
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: api
{{- end }}

{{/*
Worker specific labels
*/}}
{{- define "sqlglot-backend.worker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sqlglot-backend.name" . }}-worker
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: worker
{{- end }}

{{/*
Redis specific labels
*/}}
{{- define "sqlglot-backend.redis.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sqlglot-backend.name" . }}-redis
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: redis
{{- end }}