import _ from 'lodash'
import HoneyBadger from 'honeybadger-js'
import {GIT_SHA} from 'src/shared/constants'

if (process.env.CLOUD === 'true') {
  HoneyBadger.configure({
    apiKey: process.env.HONEYBADGER_KEY,
    revision: GIT_SHA,
    environment: process.env.HONEYBADGER_ENV,
  })

  const override = () => {
    if (!window || !window.console) {
      return
    }

    const logError = console.error

    console.error = function(...args) {
      args.forEach(arg => {
        if (_.isError(arg)) {
          HoneyBadger.notify(arg)
        }
      })

      logError.apply(console, args)
    }
  }

  override()
}
