const SEVERITY_COLORS = {
  emergency: '#BF3D5E',
  alert: '#DC4E58',
  critical: '#F95F53',
  error: '#F48D38',
  warning: '#FFB94A',
  notice: '#4ED8A0',
  info: '#7A65F2',
  debug: '#8E91A1',
}

const DEFAULT_SEVERITY_COLOR = '#7A65F2'

export const colorForSeverity = (severity: string): string => {
  return SEVERITY_COLORS[severity] || DEFAULT_SEVERITY_COLOR
}
