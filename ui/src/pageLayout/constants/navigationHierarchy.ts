import {IconFont} from '@influxdata/clockface'
import {
  CLOUD_URL,
  CLOUD_USERS_PATH,
  CLOUD_USAGE_PATH,
  CLOUD_BILLING_PATH,
} from 'src/shared/constants'

export interface NavSubItem {
  id: string
  label: string
  link: string
  cloudExclude?: boolean
  cloudOnly?: boolean
  featureFlag?: string
}

export interface NavItem {
  id: string
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
      icon: IconFont.Disks,
      label: 'Data',
      link: `${orgPrefix}/load-data/buckets`,
      menu: [
        {
          id: 'buckets',
          label: 'Buckets',
          link: `${orgPrefix}/load-data/buckets`,
        },
        {
          id: 'telegrafs',
          label: 'Telegraf',
          link: `${orgPrefix}/load-data/telegrafs`,
        },
        {
          id: 'scrapers',
          label: 'Scrapers',
          link: `${orgPrefix}/load-data/scrapers`,
          cloudExclude: true,
        },
        {
          id: 'tokens',
          label: 'Tokens',
          link: `${orgPrefix}/load-data/tokens`,
        },
        {
          id: 'client-libraries',
          label: 'Tokens',
          link: `${orgPrefix}/load-data/client-libraries`,
        },
      ],
    },
    {
      id: 'data-explorer',
      icon: IconFont.GraphLine,
      label: 'Explore',
      link: `${orgPrefix}/data-explorer`,
    },
    {
      id: 'settings',
      icon: IconFont.UsersDuo,
      label: 'Org',
      link: `${orgPrefix}/settings/members`,
      menu: [
        {
          id: 'members',
          label: 'Members',
          link: `${orgPrefix}/settings/members`,
        },
        {
          id: 'multi-user-members',
          label: 'Org Members',
          featureFlag: 'multiUser',
          link: `${CLOUD_URL}/organizations/${orgID}/${CLOUD_USERS_PATH}`,
        },
        {
          id: 'usage',
          label: 'Usage',
          link: CLOUD_USAGE_PATH,
          cloudOnly: true,
        },
        {
          id: 'billing',
          label: 'Billing',
          link: CLOUD_BILLING_PATH,
          cloudOnly: true,
        },
        {
          id: 'profile',
          label: 'Profile',
          link: `${orgPrefix}/settings/profile`,
        },
        {
          id: 'variables',
          label: 'Variables',
          link: `${orgPrefix}/settings/variables`,
        },
        {
          id: 'templates',
          label: 'Templates',
          link: `${orgPrefix}/settings/templates`,
        },
        {
          id: 'labels',
          label: 'Labels',
          link: `${orgPrefix}/settings/labels`,
        },
      ],
    },
    {
      id: 'dashboards',
      icon: IconFont.Dashboards,
      label: 'Boards',
      link: `${orgPrefix}/dashboards`,
    },
    {
      id: 'tasks',
      icon: IconFont.Calendar,
      label: 'Tasks',
      link: `${orgPrefix}/tasks`,
    },
    {
      id: 'alerting',
      icon: IconFont.Bell,
      label: 'Alerts',
      link: `${orgPrefix}/alerting`,
      menu: [
        {
          id: 'history',
          label: 'Alert History',
          link: `${orgPrefix}/alert-history`,
        },
      ],
    },
  ]
}
