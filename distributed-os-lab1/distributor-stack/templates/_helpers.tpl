{{- define "distributor-stack.fullname" -}}
{{- if .Values.nameOverride }}
{{- .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "distributor-stack.distributorName" -}}
{{- printf "%s-distributor" (include "distributor-stack.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "distributor-stack.labels" -}}
app.kubernetes.io/name: {{ include "distributor-stack.fullname" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
