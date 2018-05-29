export const config = {
  data: {
    link: {
      rel: 'self',
      href: '/kapacitor/v1/config',
    },
    sections: {
      alerta: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/alerta',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/alerta/',
            },
            options: {
              enabled: true,
              environment: 'alertaalerta',
              'insecure-skip-verify': false,
              origin: 'alerta',
              token: true,
              'token-prefix': '',
              url: 'alerta',
            },
            redacted: ['token'],
          },
        ],
      },
      hipchat: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/hipchat',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/hipchat/',
            },
            options: {
              enabled: true,
              global: false,
              room: 'hipchat',
              'state-changes-only': false,
              token: true,
              url: 'https://hipchat.hipchat.com/v2/room',
            },
            redacted: ['token'],
          },
        ],
      },
      influxdb: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/influxdb',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/influxdb/default',
            },
            options: {
              default: false,
              'disable-subscriptions': false,
              enabled: true,
              'excluded-subscriptions': {
                _kapacitor: ['autogen'],
              },
              'http-port': 0,
              'insecure-skip-verify': false,
              'kapacitor-hostname': '',
              name: 'default',
              password: false,
              'ssl-ca': '',
              'ssl-cert': '',
              'ssl-key': '',
              'startup-timeout': '5m0s',
              'subscription-protocol': 'http',
              subscriptions: null,
              'subscriptions-sync-interval': '1m0s',
              timeout: '0s',
              'udp-bind': '',
              'udp-buffer': 1000,
              'udp-read-buffer': 0,
              urls: ['http://localhost:8086'],
              username: '',
            },
            redacted: ['password'],
          },
        ],
      },
      kubernetes: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/kubernetes',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/kubernetes/',
            },
            options: {
              'api-servers': [''],
              'ca-path': '',
              enabled: false,
              id: '',
              'in-cluster': false,
              namespace: '',
              resource: '',
              token: false,
            },
            redacted: ['token'],
          },
        ],
      },
      opsgenie: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/opsgenie',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/opsgenie/',
            },
            options: {
              'api-key': true,
              enabled: true,
              global: false,
              recipients: [],
              recovery_url: 'https://api.opsgenie.com/v1/json/alert/note',
              teams: [],
              url: 'https://api.opsgenie.com/v1/json/alert',
            },
            redacted: ['api-key'],
          },
        ],
      },
      pagerduty: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/pagerduty',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/pagerduty/',
            },
            options: {
              enabled: true,
              global: false,
              'service-key': true,
              url:
                'https://events.pagerduty.com/generic/2010-04-15/create_event.json',
            },
            redacted: ['service-key'],
          },
        ],
      },
      pushover: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/pushover',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/pushover/',
            },
            options: {
              enabled: true,
              token: true,
              url: 'https://api.pushover.net/1/messages.json',
              'user-key': true,
            },
            redacted: ['token', 'user-key'],
          },
        ],
      },
      sensu: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/sensu',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/sensu/',
            },
            options: {
              addr: 'sensu',
              enabled: true,
              handlers: null,
              source: 'Kapacitor',
            },
            redacted: null,
          },
        ],
      },
      slack: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/slack',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/slack/',
            },
            options: {
              channel: 'asdf',
              enabled: true,
              global: false,
              'icon-emoji': '',
              'insecure-skip-verify': false,
              'ssl-ca': '',
              'ssl-cert': '',
              'ssl-key': '',
              'state-changes-only': false,
              url: true,
              username: 'kapacitor',
            },
            redacted: ['url'],
          },
        ],
      },
      smtp: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/smtp',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/smtp/',
            },
            options: {
              enabled: true,
              from: 'smtp@smtp.com',
              global: false,
              host: 'localhost',
              'idle-timeout': '30s',
              'no-verify': false,
              password: true,
              port: 25,
              'state-changes-only': false,
              to: null,
              username: 'smtp',
            },
            redacted: ['password'],
          },
        ],
      },
      snmptrap: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/snmptrap',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/snmptrap/',
            },
            options: {
              addr: 'localhost:162',
              community: true,
              enabled: false,
              retries: 1,
            },
            redacted: ['community'],
          },
        ],
      },
      talk: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/talk',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/talk/',
            },
            options: {
              author_name: 'talk',
              enabled: true,
              url: true,
            },
            redacted: ['url'],
          },
        ],
      },
      telegram: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/telegram',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/telegram/',
            },
            options: {
              'chat-id': 'telegram',
              'disable-notification': true,
              'disable-web-page-preview': true,
              enabled: true,
              global: false,
              'parse-mode': 'Markdown',
              'state-changes-only': false,
              token: true,
              url: 'https://api.telegram.org/bot',
            },
            redacted: ['token'],
          },
        ],
      },
      victorops: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/victorops',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/victorops/',
            },
            options: {
              'api-key': true,
              enabled: true,
              global: false,
              'routing-key': 'victorops',
              url:
                'https://alert.victorops.com/integrations/generic/20131114/alert',
            },
            redacted: ['api-key'],
          },
        ],
      },
    },
  },
  status: 200,
  statusText: 'OK',
  headers: {
    'x-kapacitor-version': 'v1.3.3',
    date: 'Sat, 09 Dec 2017 00:04:12 GMT',
    'content-encoding': 'gzip',
    'request-id': '7cbcdc60-dc74-11e7-abeb-000000000000',
    'content-length': '1121',
    'x-chronograf-version': '1.3.8.0-1002-g59bb3f9e',
    'content-type': 'application/json; charset=utf-8',
  },
  config: {
    transformRequest: {},
    transformResponse: {},
    headers: {
      Accept: 'application/json, text/plain, */*',
    },
    timeout: 0,
    xsrfCookieName: 'XSRF-TOKEN',
    xsrfHeaderName: 'X-XSRF-TOKEN',
    maxContentLength: -1,
    method: 'GET',
    url: '/chronograf/v1/sources/20/kapacitors/8/proxy',
    data: '',
    params: {
      path: '/kapacitor/v1/config',
    },
  },
  request: {},
  auth: {
    links: [],
  },
  external: {
    statusFeed: 'https://www.influxdata.com/feed/json',
  },
  meLink: '/chronograf/v1/me',
}

export const configResponse = [
  {
    type: 'alerta',
    enabled: true,
    environment: 'alertaalerta',
    origin: 'alerta',
    token: true,
  },
  {
    type: 'hipChat',
    enabled: true,
    url: 'https://hipchat.hipchat.com/v2/room',
    room: 'hipchat',
    token: true,
  },
  {
    type: 'opsGenie',
    enabled: true,
    'api-key': true,
    teams: [],
    recipients: [],
  },
  {
    type: 'pagerDuty',
    enabled: true,
    serviceKey: true,
  },
  {
    type: 'pushover',
    enabled: true,
    token: true,
    userKey: true,
  },
  {
    type: 'sensu',
    enabled: true,
    addr: 'sensu',
    source: 'Kapacitor',
  },
  {
    type: 'slack',
    enabled: true,
    url: true,
    channel: 'asdf',
  },
  {
    type: 'email',
    enabled: true,
    from: 'smtp@smtp.com',
    host: 'localhost',
    password: true,
    port: 25,
    username: 'smtp',
  },
  {
    type: 'talk',
    enabled: true,
    url: true,
    author_name: 'talk',
  },
  {
    type: 'telegram',
    enabled: true,
    token: true,
    chatId: 'telegram',
    parseMode: 'Markdown',
    disableWebPagePreview: true,
    disableNotification: true,
  },
  {
    type: 'victorOps',
    enabled: true,
    'api-key': true,
    routingKey: 'victorops',
  },
]

export const emptyConfig = {
  data: {
    link: {
      rel: 'self',
      href: '/kapacitor/v1/config',
    },
    sections: {
      alerta: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/alerta',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/alerta/',
            },
            options: {
              enabled: false,
              environment: '',
              'insecure-skip-verify': false,
              origin: '',
              token: false,
              'token-prefix': '',
              url: '',
            },
            redacted: ['token'],
          },
        ],
      },
      hipchat: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/hipchat',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/hipchat/',
            },
            options: {
              enabled: false,
              global: false,
              room: '',
              'state-changes-only': false,
              token: false,
              url: '',
            },
            redacted: ['token'],
          },
        ],
      },
      influxdb: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/influxdb',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/influxdb/default',
            },
            options: {
              default: false,
              'disable-subscriptions': false,
              enabled: true,
              'excluded-subscriptions': {
                _kapacitor: ['autogen'],
              },
              'http-port': 0,
              'insecure-skip-verify': false,
              'kapacitor-hostname': '',
              name: 'default',
              password: false,
              'ssl-ca': '',
              'ssl-cert': '',
              'ssl-key': '',
              'startup-timeout': '5m0s',
              'subscription-protocol': 'http',
              subscriptions: null,
              'subscriptions-sync-interval': '1m0s',
              timeout: '0s',
              'udp-bind': '',
              'udp-buffer': 1000,
              'udp-read-buffer': 0,
              urls: ['http://localhost:8086'],
              username: '',
            },
            redacted: ['password'],
          },
        ],
      },
      kubernetes: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/kubernetes',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/kubernetes/',
            },
            options: {
              'api-servers': [''],
              'ca-path': '',
              enabled: false,
              id: '',
              'in-cluster': false,
              namespace: '',
              resource: '',
              token: false,
            },
            redacted: ['token'],
          },
        ],
      },
      opsgenie: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/opsgenie',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/opsgenie/',
            },
            options: {
              'api-key': false,
              enabled: false,
              global: false,
              recipients: null,
              recovery_url: 'https://api.opsgenie.com/v1/json/alert/note',
              teams: null,
              url: 'https://api.opsgenie.com/v1/json/alert',
            },
            redacted: ['api-key'],
          },
        ],
      },
      pagerduty: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/pagerduty',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/pagerduty/',
            },
            options: {
              enabled: false,
              global: false,
              'service-key': false,
              url:
                'https://events.pagerduty.com/generic/2010-04-15/create_event.json',
            },
            redacted: ['service-key'],
          },
        ],
      },
      pushover: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/pushover',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/pushover/',
            },
            options: {
              enabled: false,
              token: false,
              url: 'https://api.pushover.net/1/messages.json',
              'user-key': false,
            },
            redacted: ['token', 'user-key'],
          },
        ],
      },
      sensu: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/sensu',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/sensu/',
            },
            options: {
              addr: '',
              enabled: false,
              handlers: null,
              source: 'Kapacitor',
            },
            redacted: null,
          },
        ],
      },
      slack: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/slack',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/slack/',
            },
            options: {
              channel: '',
              enabled: false,
              global: false,
              'icon-emoji': '',
              'insecure-skip-verify': false,
              'ssl-ca': '',
              'ssl-cert': '',
              'ssl-key': '',
              'state-changes-only': false,
              url: false,
              username: 'kapacitor',
            },
            redacted: ['url'],
          },
        ],
      },
      smtp: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/smtp',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/smtp/',
            },
            options: {
              enabled: false,
              from: '',
              global: false,
              host: 'localhost',
              'idle-timeout': '30s',
              'no-verify': false,
              password: false,
              port: 25,
              'state-changes-only': false,
              to: null,
              username: '',
            },
            redacted: ['password'],
          },
        ],
      },
      snmptrap: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/snmptrap',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/snmptrap/',
            },
            options: {
              addr: 'localhost:162',
              community: true,
              enabled: false,
              retries: 1,
            },
            redacted: ['community'],
          },
        ],
      },
      talk: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/talk',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/talk/',
            },
            options: {
              author_name: '',
              enabled: false,
              url: false,
            },
            redacted: ['url'],
          },
        ],
      },
      telegram: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/telegram',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/telegram/',
            },
            options: {
              'chat-id': '',
              'disable-notification': false,
              'disable-web-page-preview': false,
              enabled: false,
              global: false,
              'parse-mode': '',
              'state-changes-only': false,
              token: false,
              url: 'https://api.telegram.org/bot',
            },
            redacted: ['token'],
          },
        ],
      },
      victorops: {
        link: {
          rel: 'self',
          href: '/kapacitor/v1/config/victorops',
        },
        elements: [
          {
            link: {
              rel: 'self',
              href: '/kapacitor/v1/config/victorops/',
            },
            options: {
              'api-key': false,
              enabled: false,
              global: false,
              'routing-key': '',
              url:
                'https://alert.victorops.com/integrations/generic/20131114/alert',
            },
            redacted: ['api-key'],
          },
        ],
      },
    },
  },
  status: 200,
  statusText: 'OK',
  headers: {
    'x-kapacitor-version': 'v1.3.3',
    date: 'Sat, 09 Dec 2017 02:03:48 GMT',
    'content-encoding': 'gzip',
    'request-id': '31e90f0a-dc85-11e7-801f-000000000000',
    'content-length': '1060',
    'x-chronograf-version': '1.3.8.0-1002-g59bb3f9e',
    'content-type': 'application/json; charset=utf-8',
  },
  config: {
    transformRequest: {},
    transformResponse: {},
    headers: {
      Accept: 'application/json, text/plain, */*',
    },
    timeout: 0,
    xsrfCookieName: 'XSRF-TOKEN',
    xsrfHeaderName: 'X-XSRF-TOKEN',
    maxContentLength: -1,
    method: 'GET',
    url: '/chronograf/v1/sources/21/kapacitors/7/proxy',
    data: '',
    params: {
      path: '/kapacitor/v1/config',
    },
  },
  request: {},
  auth: {
    links: [],
  },
  external: {
    statusFeed: 'https://www.influxdata.com/feed/json',
  },
  meLink: '/chronograf/v1/me',
}

export const emptyConfigResponse = [
  {
    type: 'alerta',
    enabled: false,
    environment: '',
    origin: '',
    token: false,
  },
  {
    type: 'hipChat',
    enabled: false,
    url: '',
    room: '',
    token: false,
  },
  {
    type: 'opsGenie',
    enabled: false,
    'api-key': false,
    teams: null,
    recipients: null,
  },
  {
    type: 'pagerDuty',
    enabled: false,
    serviceKey: false,
  },
  {
    type: 'pushover',
    enabled: false,
    token: false,
    userKey: false,
  },
  {
    type: 'sensu',
    enabled: false,
    addr: '',
    source: 'Kapacitor',
  },
  {
    type: 'slack',
    enabled: false,
    url: false,
    channel: '',
  },
  {
    type: 'email',
    enabled: false,
    from: '',
    host: 'localhost',
    password: false,
    port: 25,
    username: '',
  },
  {
    type: 'talk',
    enabled: false,
    url: false,
    author_name: '',
  },
  {
    type: 'telegram',
    enabled: false,
    token: false,
    chatId: '',
    parseMode: '',
    disableWebPagePreview: false,
    disableNotification: false,
  },
  {
    type: 'victorOps',
    enabled: false,
    'api-key': false,
    routingKey: '',
  },
]

export const emptyRule = {
  id: 'DEFAULT_RULE_ID',
  queryID: '4ed63018-4ef2-4d56-95d7-7a813dd9cd04',
  trigger: 'threshold',
  values: {
    operator: 'greater than',
    value: '',
    rangeValue: '',
    relation: 'once',
    percentile: '90',
  },
  message: '',
  alertNodes: {},
  every: null,
  name: 'Untitled Rule',
}

export const handlersfromConfig = [
  {
    type: 'alerta',
    enabled: true,
    environment: 'alerta',
    origin: 'alerta',
    token: true,
  },
  {
    type: 'hipChat',
    enabled: true,
    url: 'https://hipchat.hipchat.com/v2/room',
    room: 'hipchat',
    token: true,
  },
  {
    type: 'opsGenie',
    enabled: true,
    'api-key': true,
    teams: [],
    recipients: [],
  },
  {
    type: 'pagerDuty',
    enabled: true,
    serviceKey: true,
  },
  {
    type: 'pushover',
    enabled: true,
    token: true,
    userKey: true,
  },
  {
    type: 'sensu',
    enabled: true,
    addr: 'sensu',
    source: 'Kapacitor',
  },
  {
    type: 'slack',
    enabled: true,
    url: true,
    channel: 'slack',
  },
  {
    type: 'email',
    enabled: true,
    from: 'smtp@smtp.com',
    host: 'localhost',
    password: true,
    port: 25,
    username: 'smtp',
  },
  {
    type: 'talk',
    enabled: true,
    url: true,
    author_name: 'talk',
  },
  {
    type: 'telegram',
    enabled: true,
    token: true,
    chatId: 'telegram',
    parseMode: 'Markdown',
    disableWebPagePreview: true,
    disableNotification: true,
  },
  {
    type: 'victorOps',
    enabled: true,
    'api-key': true,
    routingKey: 'victorops',
  },
]

export const handlersOnThisAlertExpected = [
  {
    enabled: true,
    url: 'http://example.com',
    headerKey: 'key',
    headerValue: 'val',
    alias: 'post-1',
    type: 'post',
  },
  {
    enabled: true,
    address: 'exampleendpoint.com:8082',
    alias: 'tcp-1',
    type: 'tcp',
  },
  {
    enabled: true,
    to: ['bob@domain.com'],
    alias: 'email-1',
    type: 'email',
  },
  {
    enabled: true,
    to: ['asdfsdf'],
    alias: 'email-2',
    type: 'email',
  },
  {
    enabled: true,
    command: ['command', 'arg'],
    alias: 'exec-1',
    type: 'exec',
  },
  {
    enabled: true,
    filePath: '/tmp/log',
    alias: 'log-1',
    type: 'log',
  },
  {
    enabled: true,
    routingKey: 'victoropsasdf',
    alias: 'victorOps-1',
    type: 'victorOps',
  },
  {
    enabled: true,
    serviceKey: '',
    alias: 'pagerDuty-1',
    type: 'pagerDuty',
  },
  {
    enabled: true,
    userKey: '',
    device: 'asdf',
    title: 'asdf',
    url: '',
    urlTitle: '',
    sound: 'asdf',
    alias: 'pushover-1',
    type: 'pushover',
  },
  {
    enabled: true,
    source: 'Kapacitor',
    handlers: ['asdf'],
    alias: 'sensu-1',
    type: 'sensu',
  },
  {
    enabled: true,
    channel: 'slack',
    username: 'asdf',
    iconEmoji: 'asdf',
    alias: 'slack-1',
    type: 'slack',
  },
  {
    enabled: true,
    chatId: 'telegram',
    parseMode: 'Markdown',
    disableWebPagePreview: false,
    disableNotification: false,
    alias: 'telegram-1',
    type: 'telegram',
  },
  {
    enabled: true,
    room: 'room',
    token: '',
    alias: 'hipChat-1',
    type: 'hipChat',
  },
  {
    enabled: true,
    token: '',
    resource: 'alerta',
    event: 'alerta',
    environment: 'alerta',
    group: 'alerta',
    value: 'alerta',
    origin: 'alerta',
    service: ['alerta'],
    alias: 'alerta-1',
    type: 'alerta',
  },
  {
    enabled: true,
    teams: ['team'],
    recipients: ['recip'],
    alias: 'opsGenie-1',
    type: 'opsGenie',
  },
  {
    enabled: true,
    teams: ['team'],
    recipients: ['team'],
    alias: 'opsGenie-2',
    type: 'opsGenie',
  },
  {
    enabled: true,
    alias: 'talk-1',
    type: 'talk',
  },
]

export const selectedHandlerExpected = {
  enabled: true,
  url: 'http://example.com',
  headerKey: 'key',
  headerValue: 'val',
  alias: 'post-1',
  type: 'post',
}
export const handlersOfKindExpected = {
  post: 1,
  tcp: 1,
  email: 2,
  exec: 1,
  log: 1,
  victorOps: 1,
  pagerDuty: 1,
  pushover: 1,
  sensu: 1,
  slack: 1,
  telegram: 1,
  hipChat: 1,
  alerta: 1,
  opsGenie: 2,
  talk: 1,
}

export const rule = {
  id: 'chronograf-v1-8e3ba5df-f5ca-4cf4-848e-7e4a4acde86e',
  tickscript:
    "var db = 'telegraf'\n\nvar rp = 'autogen'\n\nvar measurement = 'cpu'\n\nvar groupBy = []\n\nvar whereFilter = lambda: (\"host\" == 'denizs-MacBook-Pro.local')\n\nvar name = 'Untitled Rule'\n\nvar idVar = name + ':{{.Group}}'\n\nvar message = ''\n\nvar idTag = 'alertID'\n\nvar levelTag = 'level'\n\nvar messageField = 'message'\n\nvar durationField = 'duration'\n\nvar outputDB = 'chronograf'\n\nvar outputRP = 'autogen'\n\nvar outputMeasurement = 'alerts'\n\nvar triggerType = 'threshold'\n\nvar details = 'lkajsd;fl'\n\nvar crit = 30\n\nvar data = stream\n    |from()\n        .database(db)\n        .retentionPolicy(rp)\n        .measurement(measurement)\n        .groupBy(groupBy)\n        .where(whereFilter)\n    |eval(lambda: \"usage_system\")\n        .as('value')\n\nvar trigger = data\n    |alert()\n        .crit(lambda: \"value\" > crit)\n        .stateChangesOnly()\n        .message(message)\n        .id(idVar)\n        .idTag(idTag)\n        .levelTag(levelTag)\n        .messageField(messageField)\n        .durationField(durationField)\n        .details(details)\n        .post('http://example.com')\n        .header('key', 'val')\n        .tcp('exampleendpoint.com:8082')\n        .email()\n        .to('bob@domain.com')\n        .email()\n        .to('asdfsdf')\n        .exec('command', 'arg')\n        .log('/tmp/log')\n        .victorOps()\n        .routingKey('victoropsasdf')\n        .pagerDuty()\n        .pushover()\n        .device('asdf')\n        .title('asdf')\n        .sound('asdf')\n        .sensu()\n        .source('Kapacitor')\n        .handlers('asdf')\n        .slack()\n        .channel('slack')\n        .username('asdf')\n        .iconEmoji('asdf')\n        .telegram()\n        .chatId('telegram')\n        .parseMode('Markdown')\n        .hipChat()\n        .room('room')\n        .alerta()\n        .resource('alerta')\n        .event('alerta')\n        .environment('alerta')\n        .group('alerta')\n        .value('alerta')\n        .origin('alerta')\n        .services('alerta')\n        .opsGenie()\n        .teams('team')\n        .recipients('recip')\n        .opsGenie()\n        .teams('team')\n        .recipients('team')\n        .talk()\n\ntrigger\n    |eval(lambda: float(\"value\"))\n        .as('value')\n        .keep()\n    |influxDBOut()\n        .create()\n        .database(outputDB)\n        .retentionPolicy(outputRP)\n        .measurement(outputMeasurement)\n        .tag('alertName', name)\n        .tag('triggerType', triggerType)\n\ntrigger\n    |httpOut('output')\n",
  query: {
    id: 'chronograf-v1-8e3ba5df-f5ca-4cf4-848e-7e4a4acde86e',
    database: 'telegraf',
    measurement: 'cpu',
    retentionPolicy: 'autogen',
    fields: [
      {
        value: 'usage_system',
        type: 'field',
        alias: '',
      },
    ],
    tags: {
      host: ['denizs-MacBook-Pro.local'],
    },
    groupBy: {
      time: '',
      tags: [],
    },
    areTagsAccepted: true,
    rawText: null,
    range: null,
    shifts: null,
  },
  every: '',
  alertNodes: {
    typeOf: 'alert',
    stateChangesOnly: true,
    useFlapping: false,
    post: [
      {
        url: 'http://example.com',
        headers: {
          key: 'val',
        },
        headerKey: 'key',
        headerValue: 'val',
      },
    ],
    tcp: [
      {
        address: 'exampleendpoint.com:8082',
      },
    ],
    email: [
      {
        to: ['bob@domain.com'],
      },
      {
        to: ['asdfsdf'],
      },
    ],
    exec: [
      {
        command: ['command', 'arg'],
      },
    ],
    log: [
      {
        filePath: '/tmp/log',
      },
    ],
    victorOps: [
      {
        routingKey: 'victoropsasdf',
      },
    ],
    pagerDuty: [
      {
        serviceKey: '',
      },
    ],
    pushover: [
      {
        userKey: '',
        device: 'asdf',
        title: 'asdf',
        url: '',
        urlTitle: '',
        sound: 'asdf',
      },
    ],
    sensu: [
      {
        source: 'Kapacitor',
        handlers: ['asdf'],
      },
    ],
    slack: [
      {
        channel: 'slack',
        username: 'asdf',
        iconEmoji: 'asdf',
      },
    ],
    telegram: [
      {
        chatId: 'telegram',
        parseMode: 'Markdown',
        disableWebPagePreview: false,
        disableNotification: false,
      },
    ],
    hipChat: [
      {
        room: 'room',
        token: '',
      },
    ],
    alerta: [
      {
        token: '',
        resource: 'alerta',
        event: 'alerta',
        environment: 'alerta',
        group: 'alerta',
        value: 'alerta',
        origin: 'alerta',
        service: ['alerta'],
      },
    ],
    opsGenie: [
      {
        teams: ['team'],
        recipients: ['recip'],
      },
      {
        teams: ['team'],
        recipients: ['team'],
      },
    ],
    talk: [{}],
  },
  message: '',
  details: 'lkajsd;fl',
  trigger: 'threshold',
  values: {
    operator: 'greater than',
    value: '30',
    rangeValue: '',
  },
  name: 'Untitled Rule',
  type: 'stream',
  dbrps: [
    {
      db: 'telegraf',
      rp: 'autogen',
    },
  ],
  status: 'enabled',
  executing: true,
  error: '',
  created: '2017-12-08T18:38:15.27016406-08:00',
  modified: '2017-12-08T18:54:22.697657606-08:00',
  'last-enabled': '2017-12-08T18:54:22.697657606-08:00',
  links: {
    self:
      '/chronograf/v1/sources/21/kapacitors/7/rules/chronograf-v1-8e3ba5df-f5ca-4cf4-848e-7e4a4acde86e',
    kapacitor:
      '/chronograf/v1/sources/21/kapacitors/7/proxy?path=%2Fkapacitor%2Fv1%2Ftasks%2Fchronograf-v1-8e3ba5df-f5ca-4cf4-848e-7e4a4acde86e',
    output:
      '/chronograf/v1/sources/21/kapacitors/7/proxy?path=%2Fkapacitor%2Fv1%2Ftasks%2Fchronograf-v1-8e3ba5df-f5ca-4cf4-848e-7e4a4acde86e%2Foutput',
  },
  queryID: 'chronograf-v1-8e3ba5df-f5ca-4cf4-848e-7e4a4acde86e',
}

// prettier-ignore
export const RESPONSE_METADATA = `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,2018-05-23T17:42:29.536834648Z,2018-05-23T17:43:29.536834648Z,2018-05-23T17:42:29.654Z,0,usage_guest,cpu,cpu-total,WattsInfluxDB
`

export const RESPONSE_NO_METADATA = `,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,2018-05-23T17:42:29.536834648Z,2018-05-23T17:43:29.536834648Z,2018-05-23T17:42:29.654Z,0,usage_guest,cpu,cpu-total,WattsInfluxDB

`

export const RESPONSE_NO_MEASUREMENT = `,result,table,_start,_stop,_time,_value,_field,cpu,host
,,0,2018-05-23T17:42:29.536834648Z,2018-05-23T17:43:29.536834648Z,2018-05-23T17:42:29.654Z,0,usage_guest,cpu-total,WattsInfluxDB`

export const EXPECTED_COLUMNS = [
  '',
  'result',
  'table',
  '_start',
  '_stop',
  '_time',
  '_value',
  '_field',
  '_measurement',
  'cpu',
  'host',
]

export const EXPECTED_METADATA = [
  [
    'datatype',
    'string',
    'long',
    'dateTime:RFC3339',
    'dateTime:RFC3339',
    'dateTime:RFC3339',
    'double',
    'string',
    'string',
    'string',
    'string',
  ],
  [
    'partition',
    'false',
    'false',
    'false',
    'false',
    'false',
    'false',
    'true',
    'true',
    'true',
    'true',
  ],
  ['default', '_result', '', '', '', '', '', '', '', '', ''],
]

// prettier-ignore
export const LARGE_RESPONSE = `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu-total,WattsInfluxDB
,,1,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu-total,WattsInfluxDB
,,2,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,70.76923076923077,usage_idle,cpu,cpu-total,WattsInfluxDB
,,3,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu-total,WattsInfluxDB
,,4,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu-total,WattsInfluxDB
,,5,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu-total,WattsInfluxDB
,,6,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu-total,WattsInfluxDB
,,7,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu-total,WattsInfluxDB
,,8,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,11.794871794871796,usage_system,cpu,cpu-total,WattsInfluxDB
,,9,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,17.435897435897434,usage_user,cpu,cpu-total,WattsInfluxDB
,,10,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu0,WattsInfluxDB
,,11,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu0,WattsInfluxDB
,,12,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,47.82608695652174,usage_idle,cpu,cpu0,WattsInfluxDB
,,13,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu0,WattsInfluxDB
,,14,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu0,WattsInfluxDB
,,15,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu0,WattsInfluxDB
,,16,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu0,WattsInfluxDB
,,17,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu0,WattsInfluxDB
,,18,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,26.08695652173913,usage_system,cpu,cpu0,WattsInfluxDB
,,19,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,26.08695652173913,usage_user,cpu,cpu0,WattsInfluxDB
,,20,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu1,WattsInfluxDB
,,21,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu1,WattsInfluxDB
,,22,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,92,usage_idle,cpu,cpu1,WattsInfluxDB
,,23,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu1,WattsInfluxDB
,,24,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu1,WattsInfluxDB
,,25,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu1,WattsInfluxDB
,,26,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu1,WattsInfluxDB
,,27,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu1,WattsInfluxDB
,,28,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,4,usage_system,cpu,cpu1,WattsInfluxDB
,,29,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,4,usage_user,cpu,cpu1,WattsInfluxDB
,,30,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu2,WattsInfluxDB
,,31,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu2,WattsInfluxDB
,,32,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,52,usage_idle,cpu,cpu2,WattsInfluxDB
,,33,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu2,WattsInfluxDB
,,34,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu2,WattsInfluxDB
,,35,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu2,WattsInfluxDB
,,36,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu2,WattsInfluxDB
,,37,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu2,WattsInfluxDB
,,38,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,16,usage_system,cpu,cpu2,WattsInfluxDB
,,39,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,32,usage_user,cpu,cpu2,WattsInfluxDB
,,40,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu3,WattsInfluxDB
,,41,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu3,WattsInfluxDB
,,42,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,91.66666666666667,usage_idle,cpu,cpu3,WattsInfluxDB
,,43,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu3,WattsInfluxDB
,,44,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu3,WattsInfluxDB
,,45,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu3,WattsInfluxDB
,,46,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu3,WattsInfluxDB
,,47,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu3,WattsInfluxDB
,,48,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,4.166666666666667,usage_system,cpu,cpu3,WattsInfluxDB
,,49,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,4.166666666666667,usage_user,cpu,cpu3,WattsInfluxDB
,,50,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu4,WattsInfluxDB
,,51,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu4,WattsInfluxDB
,,52,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,45.833333333333336,usage_idle,cpu,cpu4,WattsInfluxDB
,,53,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu4,WattsInfluxDB
,,54,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu4,WattsInfluxDB
,,55,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu4,WattsInfluxDB
,,56,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu4,WattsInfluxDB
,,57,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu4,WattsInfluxDB
,,58,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,16.666666666666668,usage_system,cpu,cpu4,WattsInfluxDB
,,59,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,37.5,usage_user,cpu,cpu4,WattsInfluxDB
,,60,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu5,WattsInfluxDB
,,61,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu5,WattsInfluxDB
,,62,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,92,usage_idle,cpu,cpu5,WattsInfluxDB
,,63,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu5,WattsInfluxDB
,,64,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu5,WattsInfluxDB
,,65,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu5,WattsInfluxDB
,,66,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu5,WattsInfluxDB
,,67,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu5,WattsInfluxDB
,,68,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,4,usage_system,cpu,cpu5,WattsInfluxDB
,,69,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,4,usage_user,cpu,cpu5,WattsInfluxDB
,,70,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu6,WattsInfluxDB
,,71,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu6,WattsInfluxDB
,,72,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,52,usage_idle,cpu,cpu6,WattsInfluxDB
,,73,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu6,WattsInfluxDB
,,74,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu6,WattsInfluxDB
,,75,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu6,WattsInfluxDB
,,76,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu6,WattsInfluxDB
,,77,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu6,WattsInfluxDB
,,78,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,20,usage_system,cpu,cpu6,WattsInfluxDB
,,79,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,28,usage_user,cpu,cpu6,WattsInfluxDB
,,80,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu7,WattsInfluxDB
,,81,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu7,WattsInfluxDB
,,82,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,91.66666666666667,usage_idle,cpu,cpu7,WattsInfluxDB
,,83,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu7,WattsInfluxDB
,,84,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu7,WattsInfluxDB
,,85,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu7,WattsInfluxDB
,,86,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu7,WattsInfluxDB
,,87,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu7,WattsInfluxDB
,,88,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,4.166666666666667,usage_system,cpu,cpu7,WattsInfluxDB
,,89,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,4.166666666666667,usage_user,cpu,cpu7,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,90,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T21:05:08.947Z,182180679680,free,disk,/Users/watts/Downloads/TablePlus.app,nullfs,WattsInfluxDB,ro,/private/var/folders/f4/zd7n1rqj7xj6w7c0njkmmjlh0000gn/T/AppTranslocation/F4D8D166-F848-4862-94F6-B51C00E2EB7A
,,91,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T21:05:08.947Z,9223372036852008920,inodes_free,disk,/Users/watts/Downloads/TablePlus.app,nullfs,WattsInfluxDB,ro,/private/var/folders/f4/zd7n1rqj7xj6w7c0njkmmjlh0000gn/T/AppTranslocation/F4D8D166-F848-4862-94F6-B51C00E2EB7A
,,92,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T21:05:08.947Z,9223372036854775807,inodes_total,disk,/Users/watts/Downloads/TablePlus.app,nullfs,WattsInfluxDB,ro,/private/var/folders/f4/zd7n1rqj7xj6w7c0njkmmjlh0000gn/T/AppTranslocation/F4D8D166-F848-4862-94F6-B51C00E2EB7A
,,93,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T21:05:08.947Z,2766887,inodes_used,disk,/Users/watts/Downloads/TablePlus.app,nullfs,WattsInfluxDB,ro,/private/var/folders/f4/zd7n1rqj7xj6w7c0njkmmjlh0000gn/T/AppTranslocation/F4D8D166-F848-4862-94F6-B51C00E2EB7A
,,94,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T21:05:08.947Z,499963170816,total,disk,/Users/watts/Downloads/TablePlus.app,nullfs,WattsInfluxDB,ro,/private/var/folders/f4/zd7n1rqj7xj6w7c0njkmmjlh0000gn/T/AppTranslocation/F4D8D166-F848-4862-94F6-B51C00E2EB7A
,,95,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T21:05:08.947Z,314933657600,used,disk,/Users/watts/Downloads/TablePlus.app,nullfs,WattsInfluxDB,ro,/private/var/folders/f4/zd7n1rqj7xj6w7c0njkmmjlh0000gn/T/AppTranslocation/F4D8D166-F848-4862-94F6-B51C00E2EB7A

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,96,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T21:05:08.947Z,63.352358598865635,used_percent,disk,/Users/watts/Downloads/TablePlus.app,nullfs,WattsInfluxDB,ro,/private/var/folders/f4/zd7n1rqj7xj6w7c0njkmmjlh0000gn/T/AppTranslocation/F4D8D166-F848-4862-94F6-B51C00E2EB7A

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,97,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,free,disk,devfs,devfs,WattsInfluxDB,rw,/dev
,,98,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,inodes_free,disk,devfs,devfs,WattsInfluxDB,rw,/dev
,,99,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,716,inodes_total,disk,devfs,devfs,WattsInfluxDB,rw,/dev
,,100,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,716,inodes_used,disk,devfs,devfs,WattsInfluxDB,rw,/dev
,,101,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,211968,total,disk,devfs,devfs,WattsInfluxDB,rw,/dev
,,102,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,211968,used,disk,devfs,devfs,WattsInfluxDB,rw,/dev

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,103,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,100,used_percent,disk,devfs,devfs,WattsInfluxDB,rw,/dev

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,104,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-18T03:13:34.143Z,258453504,free,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/bless.9mnm
,,105,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-18T03:13:34.143Z,0,inodes_free,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/bless.9mnm
,,106,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-18T03:13:34.143Z,0,inodes_total,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/bless.9mnm
,,107,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-18T03:13:34.143Z,0,inodes_used,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/bless.9mnm
,,108,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-18T03:13:34.143Z,313827328,total,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/bless.9mnm
,,109,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-18T03:13:34.143Z,55373824,used,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/bless.9mnm

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,110,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-18T03:13:34.143Z,17.644678796105353,used_percent,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/bless.9mnm

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,111,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-12T05:09:50.004Z,274092032,free,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/firmwaresyncd.ushpRB
,,112,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-12T05:09:50.004Z,0,inodes_free,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/firmwaresyncd.ushpRB
,,113,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-12T05:09:50.004Z,0,inodes_total,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/firmwaresyncd.ushpRB
,,114,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-12T05:09:50.004Z,0,inodes_used,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/firmwaresyncd.ushpRB
,,115,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-12T05:09:50.004Z,313827328,total,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/firmwaresyncd.ushpRB
,,116,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-12T05:09:50.004Z,39735296,used,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/firmwaresyncd.ushpRB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,117,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-12T05:09:50.004Z,12.66151557075361,used_percent,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/firmwaresyncd.ushpRB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,118,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,220499697664,free,disk,disk1s1,apfs,WattsInfluxDB,rw,/
,,119,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,9223372036852024886,inodes_free,disk,disk1s1,apfs,WattsInfluxDB,rw,/
,,120,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,9223372036854775807,inodes_total,disk,disk1s1,apfs,WattsInfluxDB,rw,/
,,121,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,2750921,inodes_used,disk,disk1s1,apfs,WattsInfluxDB,rw,/
,,122,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,499963170816,total,disk,disk1s1,apfs,WattsInfluxDB,rw,/
,,123,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,275540910080,used,disk,disk1s1,apfs,WattsInfluxDB,rw,/

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,124,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,55.54805509435289,used_percent,disk,disk1s1,apfs,WattsInfluxDB,rw,/

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,125,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-20T04:05:50.608Z,171371986944,free,disk,disk1s2,apfs,WattsInfluxDB,rw,/Volumes/Preboot 1
,,126,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-20T04:05:50.608Z,9223372036854775741,inodes_free,disk,disk1s2,apfs,WattsInfluxDB,rw,/Volumes/Preboot 1
,,127,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-20T04:05:50.608Z,9223372036854775807,inodes_total,disk,disk1s2,apfs,WattsInfluxDB,rw,/Volumes/Preboot 1
,,128,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-20T04:05:50.608Z,66,inodes_used,disk,disk1s2,apfs,WattsInfluxDB,rw,/Volumes/Preboot 1
,,129,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-20T04:05:50.608Z,499963170816,total,disk,disk1s2,apfs,WattsInfluxDB,rw,/Volumes/Preboot 1
,,130,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-20T04:05:50.608Z,21532672,used,disk,disk1s2,apfs,WattsInfluxDB,rw,/Volumes/Preboot 1

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,131,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-20T04:05:50.608Z,0.012563294136349525,used_percent,disk,disk1s2,apfs,WattsInfluxDB,rw,/Volumes/Preboot 1

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,132,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-19T19:33:36.939Z,167696769024,free,disk,disk1s3,apfs,WattsInfluxDB,rw,/Volumes/Recovery
,,133,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-19T19:33:36.939Z,9223372036854775793,inodes_free,disk,disk1s3,apfs,WattsInfluxDB,rw,/Volumes/Recovery
,,134,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-19T19:33:36.939Z,9223372036854775807,inodes_total,disk,disk1s3,apfs,WattsInfluxDB,rw,/Volumes/Recovery
,,135,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-19T19:33:36.939Z,14,inodes_used,disk,disk1s3,apfs,WattsInfluxDB,rw,/Volumes/Recovery
,,136,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-19T19:33:36.939Z,499963170816,total,disk,disk1s3,apfs,WattsInfluxDB,rw,/Volumes/Recovery
,,137,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-19T19:33:36.939Z,517763072,used,disk,disk1s3,apfs,WattsInfluxDB,rw,/Volumes/Recovery

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,138,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-19T19:33:36.939Z,0.30779925226942506,used_percent,disk,disk1s3,apfs,WattsInfluxDB,rw,/Volumes/Recovery

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,139,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,220499697664,free,disk,disk1s4,apfs,WattsInfluxDB,rw,/private/var/vm
,,140,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,9223372036854775804,inodes_free,disk,disk1s4,apfs,WattsInfluxDB,rw,/private/var/vm
,,141,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,9223372036854775807,inodes_total,disk,disk1s4,apfs,WattsInfluxDB,rw,/private/var/vm
,,142,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,3,inodes_used,disk,disk1s4,apfs,WattsInfluxDB,rw,/private/var/vm
,,143,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,499963170816,total,disk,disk1s4,apfs,WattsInfluxDB,rw,/private/var/vm
,,144,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,3221266432,used,disk,disk1s4,apfs,WattsInfluxDB,rw,/private/var/vm

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,145,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,1.4398589980229728,used_percent,disk,disk1s4,apfs,WattsInfluxDB,rw,/private/var/vm

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,146,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-07T04:06:42.333Z,0,free,disk,disk2,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,147,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-07T04:06:42.333Z,3390710,inodes_free,disk,disk2,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,148,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-07T04:06:42.333Z,71,inodes_total,disk,disk2,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,149,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-07T04:06:42.333Z,0,inodes_used,disk,disk2,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,150,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-07T04:06:42.333Z,6944174080,total,disk,disk2,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,151,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-07T04:06:42.333Z,0,used,disk,disk2,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,152,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-07T04:06:42.333Z,0,used_percent,disk,disk2,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,153,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-14T16:10:41.251Z,389857280,free,disk,disk2s1,hfs,WattsInfluxDB,ro,/Volumes/FF95FBB6-192D-47B9-BBBC-833F6368D429
,,154,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-14T16:10:41.251Z,4294966864,inodes_free,disk,disk2s1,hfs,WattsInfluxDB,ro,/Volumes/FF95FBB6-192D-47B9-BBBC-833F6368D429
,,155,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-14T16:10:41.251Z,4294967279,inodes_total,disk,disk2s1,hfs,WattsInfluxDB,ro,/Volumes/FF95FBB6-192D-47B9-BBBC-833F6368D429
,,156,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-14T16:10:41.251Z,415,inodes_used,disk,disk2s1,hfs,WattsInfluxDB,ro,/Volumes/FF95FBB6-192D-47B9-BBBC-833F6368D429
,,157,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-14T16:10:41.251Z,1698652160,total,disk,disk2s1,hfs,WattsInfluxDB,ro,/Volumes/FF95FBB6-192D-47B9-BBBC-833F6368D429
,,158,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-14T16:10:41.251Z,1308794880,used,disk,disk2s1,hfs,WattsInfluxDB,ro,/Volumes/FF95FBB6-192D-47B9-BBBC-833F6368D429

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,159,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-14T16:10:41.251Z,77.04902220829013,used_percent,disk,disk2s1,hfs,WattsInfluxDB,ro,/Volumes/FF95FBB6-192D-47B9-BBBC-833F6368D429

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,160,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-17T22:33:07.478Z,0,free,disk,disk2s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.q62GM7vGxK/m
,,161,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-17T22:33:07.478Z,4294966918,inodes_free,disk,disk2s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.q62GM7vGxK/m
,,162,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-17T22:33:07.478Z,4294967279,inodes_total,disk,disk2s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.q62GM7vGxK/m
,,163,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-17T22:33:07.478Z,361,inodes_used,disk,disk2s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.q62GM7vGxK/m
,,164,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-17T22:33:07.478Z,185028608,total,disk,disk2s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.q62GM7vGxK/m
,,165,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-17T22:33:07.478Z,185028608,used,disk,disk2s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.q62GM7vGxK/m

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,166,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-17T22:33:07.478Z,100,used_percent,disk,disk2s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.q62GM7vGxK/m

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,167,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,0,free,disk,disk3,udf,WattsInfluxDB,ro,/Volumes/Thor Ragnarok
,,168,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,3426691,inodes_free,disk,disk3,udf,WattsInfluxDB,ro,/Volumes/Thor Ragnarok
,,169,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,41,inodes_total,disk,disk3,udf,WattsInfluxDB,ro,/Volumes/Thor Ragnarok
,,170,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,0,inodes_used,disk,disk3,udf,WattsInfluxDB,ro,/Volumes/Thor Ragnarok
,,171,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,7017863168,total,disk,disk3,udf,WattsInfluxDB,ro,/Volumes/Thor Ragnarok
,,172,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,0,used,disk,disk3,udf,WattsInfluxDB,ro,/Volumes/Thor Ragnarok

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,173,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,0,used_percent,disk,disk3,udf,WattsInfluxDB,ro,/Volumes/Thor Ragnarok

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,174,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-11T17:15:35.255Z,389857280,free,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/0FD2794B-226F-4DEB-A19C-75E005A6AC57
,,175,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-11T17:15:35.255Z,4294966864,inodes_free,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/0FD2794B-226F-4DEB-A19C-75E005A6AC57
,,176,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-11T17:15:35.255Z,4294967279,inodes_total,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/0FD2794B-226F-4DEB-A19C-75E005A6AC57
,,177,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-11T17:15:35.255Z,415,inodes_used,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/0FD2794B-226F-4DEB-A19C-75E005A6AC57
,,178,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-11T17:15:35.255Z,1698652160,total,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/0FD2794B-226F-4DEB-A19C-75E005A6AC57
,,179,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-11T17:15:35.255Z,1308794880,used,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/0FD2794B-226F-4DEB-A19C-75E005A6AC57

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,180,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-11T17:15:35.255Z,77.04902220829013,used_percent,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/0FD2794B-226F-4DEB-A19C-75E005A6AC57

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,181,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:46:03.067Z,105676800,free,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/RecoveryHDMeta
,,182,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:46:03.067Z,4294967273,inodes_free,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/RecoveryHDMeta
,,183,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:46:03.067Z,4294967279,inodes_total,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/RecoveryHDMeta
,,184,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:46:03.067Z,6,inodes_used,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/RecoveryHDMeta
,,185,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:46:03.067Z,609431552,total,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/RecoveryHDMeta
,,186,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:46:03.067Z,503754752,used,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/RecoveryHDMeta

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,187,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:46:03.067Z,82.65977538360207,used_percent,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/RecoveryHDMeta

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,188,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,0,free,disk,disk4,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,189,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,3390710,inodes_free,disk,disk4,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,190,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,71,inodes_total,disk,disk4,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,191,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,0,inodes_used,disk,disk4,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,192,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,6944174080,total,disk,disk4,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,193,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,0,used,disk,disk4,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,194,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,0,used_percent,disk,disk4,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,195,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-26T16:25:42.052Z,389865472,free,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/55713F97-1C88-4076-B6BB-8897592CB08B
,,196,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-26T16:25:42.052Z,4294966864,inodes_free,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/55713F97-1C88-4076-B6BB-8897592CB08B
,,197,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-26T16:25:42.052Z,4294967279,inodes_total,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/55713F97-1C88-4076-B6BB-8897592CB08B
,,198,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-26T16:25:42.052Z,415,inodes_used,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/55713F97-1C88-4076-B6BB-8897592CB08B
,,199,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-26T16:25:42.052Z,1698652160,total,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/55713F97-1C88-4076-B6BB-8897592CB08B
,,200,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-26T16:25:42.052Z,1308786688,used,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/55713F97-1C88-4076-B6BB-8897592CB08B

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,201,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-26T16:25:42.052Z,77.04853994357502,used_percent,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/55713F97-1C88-4076-B6BB-8897592CB08B

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,202,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-22T19:11:41.599Z,444579840,free,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/A81441DB-3D0C-48E8-8BED-F53FCD1D6D3C
,,203,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-22T19:11:41.599Z,4294967132,inodes_free,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/A81441DB-3D0C-48E8-8BED-F53FCD1D6D3C
,,204,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-22T19:11:41.599Z,4294967279,inodes_total,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/A81441DB-3D0C-48E8-8BED-F53FCD1D6D3C
,,205,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-22T19:11:41.599Z,147,inodes_used,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/A81441DB-3D0C-48E8-8BED-F53FCD1D6D3C
,,206,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-22T19:11:41.599Z,524247040,total,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/A81441DB-3D0C-48E8-8BED-F53FCD1D6D3C
,,207,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-22T19:11:41.599Z,79667200,used,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/A81441DB-3D0C-48E8-8BED-F53FCD1D6D3C

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,208,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-22T19:11:41.599Z,15.196499726541134,used_percent,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/A81441DB-3D0C-48E8-8BED-F53FCD1D6D3C

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,209,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-27T17:08:29.908Z,7413760,free,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/Tunnelblick
,,210,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-27T17:08:29.908Z,4294966699,inodes_free,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/Tunnelblick
,,211,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-27T17:08:29.908Z,4294967279,inodes_total,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/Tunnelblick
,,212,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-27T17:08:29.908Z,580,inodes_used,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/Tunnelblick
,,213,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-27T17:08:29.908Z,40693760,total,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/Tunnelblick
,,214,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-27T17:08:29.908Z,33280000,used,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/Tunnelblick

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,215,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-27T17:08:29.908Z,81.78158027176649,used_percent,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/Tunnelblick

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,216,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-01T19:26:01.539Z,0,free,disk,disk5s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.1EQur33ekx/m
,,217,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-01T19:26:01.539Z,4294966918,inodes_free,disk,disk5s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.1EQur33ekx/m
,,218,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-01T19:26:01.539Z,4294967279,inodes_total,disk,disk5s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.1EQur33ekx/m
,,219,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-01T19:26:01.539Z,361,inodes_used,disk,disk5s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.1EQur33ekx/m
,,220,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-01T19:26:01.539Z,185032704,total,disk,disk5s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.1EQur33ekx/m
,,221,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-01T19:26:01.539Z,185032704,used,disk,disk5s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.1EQur33ekx/m

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,222,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-01T19:26:01.539Z,100,used_percent,disk,disk5s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.1EQur33ekx/m

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#partition,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,223,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,6318931968,active,mem,WattsInfluxDB
,,224,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,5277085696,available,mem,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
#partition,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,225,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,30.716681480407715,available_percent,mem,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#partition,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,226,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,buffered,mem,WattsInfluxDB
,,227,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,cached,mem,WattsInfluxDB
,,228,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,1897549824,free,mem,WattsInfluxDB
,,229,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,3379535872,inactive,mem,WattsInfluxDB
,,230,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,slab,mem,WattsInfluxDB
,,231,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,17179869184,total,mem,WattsInfluxDB
,,232,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,11902783488,used,mem,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
#partition,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,233,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,69.28331851959229,used_percent,mem,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#partition,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,234,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,3103551488,wired,mem,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
#partition,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,235,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.589Z,8.66,load1,system,WattsInfluxDB
,,236,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.589Z,3.78,load15,system,WattsInfluxDB
,,237,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.589Z,5.35,load5,system,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#partition,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,238,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.589Z,8,n_cpus,system,WattsInfluxDB
,,239,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.589Z,11,n_users,system,WattsInfluxDB
,,240,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.34Z,90708,uptime,system,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,241,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.34Z,"1 day,  1:11",uptime_format,system,WattsInfluxDB


`
