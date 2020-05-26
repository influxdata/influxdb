export const RATE_LIMIT_ERROR_STATUS = 429

export const RATE_LIMIT_ERROR_TEXT =
  'Oops. It looks like you have exceeded the query limits allowed as part of your plan. If you would like to increase your query limits, reach out to support@influxdata.com.'

export const ASSET_LIMIT_ERROR_STATUS = 403

export const ASSET_LIMIT_ERROR_TEXT =
  'Oops. It looks like you have exceeded the asset limits allowed as part of your plan. If you would like to increase your limits, reach out to support@influxdata.com.'

const WebsiteMonitoringDashboardTemplate = async (name: string) => {
  const websiteMonitoringTemplate = await import(/* webpackPrefetch: true */ 'src/cloud/constants/websiteMonitoringTemplate')
  websiteMonitoringTemplate.default.content.data.attributes.name = name
  return websiteMonitoringTemplate.default
}

export const WebsiteMonitoringBucket = 'Website Monitoring Bucket'
export const WebsiteMonitoringDemoDataDashboard =
  'Website Monitoring Demo Data Dashboard'

export const DemoDataDashboards = {
  [WebsiteMonitoringBucket]: WebsiteMonitoringDemoDataDashboard,
}

export const DemoDataDashboardNames = {
  [WebsiteMonitoringDemoDataDashboard]: WebsiteMonitoringBucket,
}

export const DemoDataTemplates = {
  [WebsiteMonitoringBucket]: WebsiteMonitoringDashboardTemplate(
    DemoDataDashboards[WebsiteMonitoringBucket]
  ),
}

export const HIDE_UPGRADE_CTA_KEY = 'hide_upgrade_cta'
export const FREE_ORG_HIDE_UPGRADE_SETTING = {
  key: HIDE_UPGRADE_CTA_KEY,
  value: 'false',
}
export const PAID_ORG_HIDE_UPGRADE_SETTING = {
  key: HIDE_UPGRADE_CTA_KEY,
  value: 'true',
}
