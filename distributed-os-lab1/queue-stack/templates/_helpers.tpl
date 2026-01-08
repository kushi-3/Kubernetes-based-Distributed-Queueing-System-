{{- define "queue-stack.fullname" -}}
{{- if .Values.nameOverride }}
{{- .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "queue-stack.queueName" -}}
{{- printf "%s-queue" (include "queue-stack.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "queue-stack.serverName" -}}
{{- printf "%s-server" (include "queue-stack.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "queue-stack.labels" -}}
app.kubernetes.io/name: {{ include "queue-stack.fullname" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
