import {IconFont} from '@influxdata/clockface'
import {
  CLOUD_URL,
  CLOUD_USERS_PATH,
  CLOUD_USAGE_PATH,
  CLOUD_BILLING_PATH,
} from 'src/shared/constants'

export interface NavSubItem {
  id: string
  testID: string
  label: string
  link: string
  cloudExclude?: boolean
  cloudOnly?: boolean
  featureFlag?: string
}

export interface NavItem {
  id: string
  testID: string
  label: string
  link: string
  icon: IconFont
  cloudExclude?: boolean
  cloudOnly?: boolean
  featureFlag?: string
  menu?: NavSubItem[]
}

export const generateNavItems = (orgID: string): NavItem[] => {
  const orgPrefix = `/orgs/${orgID}`

  return [
    {
      id: 'load-data',
      testID: 'nav-item-load-data',
      icon: IconFont.DisksNav,
      label: 'Data',
      link: `${orgPrefix}/load-data/buckets`,
      menu: [
        {
          id: 'buckets',
          testID: 'nav-subitem-buckets',
          label: 'Buckets',
          link: `${orgPrefix}/load-data/buckets`,
        },
        {
          id: 'telegrafs',
          testID: 'nav-subitem-telegrafs',
          label: 'Telegraf',
          link: `${orgPrefix}/load-data/telegrafs`,
        },
        {
          id: 'scrapers',
          testID: 'nav-subitem-scrapers',
          label: 'Scrapers',
          link: `${orgPrefix}/load-data/scrapers`,
          cloudExclude: true,
        },
        {
          id: 'tokens',
          testID: 'nav-subitem-tokens',
          label: 'Tokens',
          link: `${orgPrefix}/load-data/tokens`,
        },
        {
          id: 'client-libraries',
          testID: 'nav-subitem-client-libraries',
          label: 'Tokens',
          link: `${orgPrefix}/load-data/client-libraries`,
        },
      ],
    },
    {
      id: 'data-explorer',
      testID: 'nav-item-data-explorer',
      icon: IconFont.GraphLine,
      label: 'Explore',
      link: `${orgPrefix}/data-explorer`,
    },
    {
      id: 'settings',
      testID: 'nav-item-settings',
      icon: IconFont.UsersDuo,
      label: 'Org',
      link: `${orgPrefix}/settings/members`,
      menu: [
        {
          id: 'members',
          testID: 'nav-subitem-members',
          label: 'Members',
          link: `${orgPrefix}/settings/members`,
        },
        {
          id: 'multi-user-members',
          testID: 'nav-subitem-multi-user-members',
          label: 'Org Members',
          featureFlag: 'multiUser',
          link: `${CLOUD_URL}/organizations/${orgID}/${CLOUD_USERS_PATH}`,
        },
        {
          id: 'usage',
          testID: 'nav-subitem-usage',
          label: 'Usage',
          link: CLOUD_USAGE_PATH,
          cloudOnly: true,
        },
        {
          id: 'billing',
          testID: 'nav-subitem-billing',
          label: 'Billing',
          link: CLOUD_BILLING_PATH,
          cloudOnly: true,
        },
        {
          id: 'profile',
          testID: 'nav-subitem-profile',
          label: 'Profile',
          link: `${orgPrefix}/settings/profile`,
        },
        {
          id: 'variables',
          testID: 'nav-subitem-variables',
          label: 'Variables',
          link: `${orgPrefix}/settings/variables`,
        },
        {
          id: 'templates',
          testID: 'nav-subitem-templates',
          label: 'Templates',
          link: `${orgPrefix}/settings/templates`,
        },
        {
          id: 'labels',
          testID: 'nav-subitem-labels',
          label: 'Labels',
          link: `${orgPrefix}/settings/labels`,
        },
      ],
    },
    {
      id: 'dashboards',
      testID: 'nav-item-dashboards',
      icon: IconFont.Dashboards,
      label: 'Boards',
      link: `${orgPrefix}/dashboards`,
    },
    {
      id: 'tasks',
      testID: 'nav-item-tasks',
      icon: IconFont.Calendar,
      label: 'Tasks',
      link: `${orgPrefix}/tasks`,
    },
    {
      id: 'alerting',
      testID: 'nav-item-alerting',
      icon: IconFont.Bell,
      label: 'Alerts',
      link: `${orgPrefix}/alerting`,
      menu: [
        {
          id: 'history',
          testID: 'nav-subitem-history',
          label: 'Alert History',
          link: `${orgPrefix}/alert-history`,
        },
      ],
    },
  ]
}
