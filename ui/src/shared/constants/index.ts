import _ from 'lodash'
import {Config} from '@influxdata/giraffe'
import {AutoRefreshStatus} from 'src/types'

export const DEFAULT_TIME_FORMAT = 'YYYY-MM-DD HH:mm:ss ZZ'

export const DEFAULT_DURATION_MS = 1000

export const DROPDOWN_MENU_MAX_HEIGHT = 240

export const PRESENTATION_MODE_ANIMATION_DELAY = 0 // In milliseconds.

export const HTTP_UNAUTHORIZED = 401
export const HTTP_FORBIDDEN = 403
export const HTTP_NOT_FOUND = 404

export const AUTOREFRESH_DEFAULT_INTERVAL = 0 // in milliseconds
export const AUTOREFRESH_DEFAULT_STATUS = AutoRefreshStatus.Paused
export const AUTOREFRESH_DEFAULT = {
  status: AUTOREFRESH_DEFAULT_STATUS,
  interval: AUTOREFRESH_DEFAULT_INTERVAL,
}

export const LAYOUT_MARGIN = 4
export const DASHBOARD_LAYOUT_ROW_HEIGHT = 83.5

export const NOTIFICATION_TRANSITION = 250
export const FIVE_SECONDS = 5000
export const TEN_SECONDS = 10000
export const INFINITE = -1

// Resizer && Threesizer
export const HANDLE_VERTICAL = 'vertical'
export const HANDLE_HORIZONTAL = 'horizontal'
export const HANDLE_NONE = 'none'
export const HANDLE_PIXELS = 20
export const MIN_HANDLE_PIXELS = 20
export const MAX_SIZE = 1
export const MIN_SIZE = 0

export const VERSION = process.env.npm_package_version
export const GIT_SHA = process.env.GIT_SHA

export const CLOUD = process.env.CLOUD && process.env.CLOUD === 'true'
export const CLOUD_SIGNIN_PATHNAME = '/api/v2/signin'
export const CLOUD_SIGNOUT_URL = process.env.CLOUD_LOGOUT_URL
export const CLOUD_BILLING_VISIBLE =
  CLOUD && process.env.CLOUD_BILLING_VISIBLE === 'true'
export const CLOUD_URL = process.env.CLOUD_URL
export const CLOUD_CHECKOUT_PATH = process.env.CLOUD_CHECKOUT_PATH

export const VIS_SIG_DIGITS = 4

export const VIS_THEME: Partial<Config> = {
  axisColor: '#31313d',
  gridColor: '#31313d',
  gridOpacity: 1,
  tickFont: 'bold 10px Roboto',
  tickFontColor: '#c6cad3',
  legendFont: '12px Roboto',
  legendFontColor: '#8e91a1',
  legendFontBrightColor: '#c6cad3',
  legendBackgroundColor: '#1c1c21',
  legendBorder: '1px solid #202028',
  legendCrosshairColor: '#434453',
}
