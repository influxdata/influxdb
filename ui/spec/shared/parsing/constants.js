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

export const handlersOnThisAlert_expected = [
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

export const selectedHandler_expected = {
  enabled: true,
  url: 'http://example.com',
  headerKey: 'key',
  headerValue: 'val',
  alias: 'post-1',
  type: 'post',
}
export const handlersOfKind_expected = {
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
