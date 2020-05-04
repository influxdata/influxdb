import {IconFont} from '@influxdata/clockface'
import {CLOUD_URL, CLOUD_USERS_PATH} from 'src/shared/constants'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

export interface NavItemLink {
  type: 'link' | 'href'
  location: string
}

export interface NavSubItem {
  id: string
  testID: string
  label: string
  link: NavItemLink
  cloudExclude?: boolean
  cloudOnly?: boolean
  featureFlag?: string
  featureFlagValue?: boolean
}

export interface NavItem {
  id: string
  testID: string
  label: string
  shortLabel?: string
  link: NavItemLink
  icon: IconFont
  cloudExclude?: boolean
  cloudOnly?: boolean
  featureFlag?: string
  featureFlagValue?: boolean
  menu?: NavSubItem[]
  activeKeywords: string[]
}

export const generateNavItems = (orgID: string): NavItem[] => {
  const orgPrefix = `/orgs/${orgID}`

  const isMultiUserEnabled = isFlagEnabled('multiUser')

  const quartzMembersHeaderLink: NavItemLink = isMultiUserEnabled
    ? {
        type: 'href',
        location: `${CLOUD_URL}/organizations/${orgID}${CLOUD_USERS_PATH}`,
      }
    : {
        type: 'link',
        location: `${orgPrefix}/about`,
      }

  return [
    {
      id: 'load-data',
      testID: 'nav-item-load-data',
      icon: IconFont.DisksNav,
      label: 'Load Data',
      shortLabel: 'Data',
      link: {
        type: 'link',
        location: `${orgPrefix}/load-data/buckets`,
      },
      activeKeywords: ['load-data'],
      menu: [
        {
          id: 'buckets',
          testID: 'nav-subitem-buckets',
          label: 'Buckets',
          link: {
            type: 'link',
            location: `${orgPrefix}/load-data/buckets`,
          },
        },
        {
          id: 'telegrafs',
          testID: 'nav-subitem-telegrafs',
          label: 'Telegraf',
          link: {
            type: 'link',
            location: `${orgPrefix}/load-data/telegrafs`,
          },
        },
        {
          id: 'scrapers',
          testID: 'nav-subitem-scrapers',
          label: 'Scrapers',
          link: {
            type: 'link',
            location: `${orgPrefix}/load-data/scrapers`,
          },
          cloudExclude: true,
        },
        {
          id: 'tokens',
          testID: 'nav-subitem-tokens',
          label: 'Tokens',
          link: {
            type: 'link',
            location: `${orgPrefix}/load-data/tokens`,
          },
        },
        {
          id: 'client-libraries',
          testID: 'nav-subitem-client-libraries',
          label: 'Client Libraries',
          link: {
            type: 'link',
            location: `${orgPrefix}/load-data/client-libraries`,
          },
        },
      ],
    },
    {
      id: 'data-explorer',
      testID: 'nav-item-data-explorer',
      icon: IconFont.GraphLine,
      label: 'Data Explorer',
      shortLabel: 'Explore',
      link: {
        type: 'link',
        location: `${orgPrefix}/data-explorer`,
      },
      activeKeywords: ['data-explorer'],
    },
    {
      id: 'notebooks',
      testID: 'nav-item-notebooks',
      icon: IconFont.Erlenmeyer,
      label: 'Notebook',
      featureFlag: 'notebooks',
      shortLabel: 'Book',
      link: {
        type: 'link',
        location: `${orgPrefix}/notebooks`,
      },
      activeKeywords: ['notebooks'],
    },
    {
      id: 'org',
      testID: 'nav-item-org',
      icon: IconFont.UsersTrio,
      label: 'Organization',
      shortLabel: 'Org',
      link: {
        type: 'link',
        location: `${orgPrefix}/members`,
      },
      cloudExclude: true,
      activeKeywords: ['members', 'about'],
      menu: [
        {
          id: 'members',
          testID: 'nav-subitem-members',
          label: 'Members',
          link: {
            type: 'link',
            location: `${orgPrefix}/members`,
          },
        },
        {
          id: 'about',
          testID: 'nav-subitem-about',
          label: 'About',
          link: {
            type: 'link',
            location: `${orgPrefix}/about`,
          },
        },
      ],
    },
    {
      id: 'org-quartz',
      testID: 'nav-item-quartz-org',
      icon: IconFont.UsersTrio,
      label: 'Organization',
      shortLabel: 'Org',
      cloudOnly: true,
      link: quartzMembersHeaderLink,
      activeKeywords: ['members', 'about'],
      menu: [
        {
          id: 'users',
          testID: 'nav-subitem-users',
          label: 'Members',
          featureFlag: 'multiUser',
          link: {
            type: 'href',
            location: `${CLOUD_URL}/organizations/${orgID}${CLOUD_USERS_PATH}`,
          },
        },
        {
          id: 'about',
          testID: 'nav-subitem-about',
          label: 'About',
          link: {
            type: 'link',
            location: `${orgPrefix}/about`,
          },
        },
      ],
    },
    {
      id: 'dashboards',
      testID: 'nav-item-dashboards',
      icon: IconFont.Dashboards,
      label: 'Dashboards',
      shortLabel: 'Boards',
      link: {
        type: 'link',
        location: `${orgPrefix}/dashboards`,
      },
      activeKeywords: ['dashboards'],
    },
    {
      id: 'tasks',
      testID: 'nav-item-tasks',
      icon: IconFont.Calendar,
      label: 'Tasks',
      link: {
        type: 'link',
        location: `${orgPrefix}/tasks`,
      },
      activeKeywords: ['tasks'],
    },
    {
      id: 'alerting',
      testID: 'nav-item-alerting',
      icon: IconFont.Bell,
      label: 'Alerts',
      link: {
        type: 'link',
        location: `${orgPrefix}/alerting`,
      },
      activeKeywords: ['alerting'],
      menu: [
        {
          id: 'history',
          testID: 'nav-subitem-history',
          label: 'Alert History',
          link: {
            type: 'link',
            location: `${orgPrefix}/alert-history`,
          },
        },
      ],
    },
    {
      id: 'settings',
      testID: 'nav-item-settings',
      icon: IconFont.WrenchNav,
      label: 'Settings',
      link: {
        type: 'link',
        location: `${orgPrefix}/settings/variables`,
      },
      activeKeywords: ['settings'],
      menu: [
        {
          id: 'variables',
          testID: 'nav-subitem-variables',
          label: 'Variables',
          link: {
            type: 'link',
            location: `${orgPrefix}/settings/variables`,
          },
        },
        {
          id: 'templates',
          testID: 'nav-subitem-templates',
          label: 'Templates',
          link: {
            type: 'link',
            location: `${orgPrefix}/settings/templates`,
          },
        },
        {
          id: 'labels',
          testID: 'nav-subitem-labels',
          label: 'Labels',
          link: {
            type: 'link',
            location: `${orgPrefix}/settings/labels`,
          },
        },
      ],
    },
  ]
}
