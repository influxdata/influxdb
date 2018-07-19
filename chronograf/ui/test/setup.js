import {configure} from 'enzyme'
import React from 'react'

import Adapter from 'enzyme-adapter-react-16'

configure({
  adapter: new Adapter(),
})

/**
 * Since React v15.5, there's a warning printed if you access `React.createClass` or `React.PropTypes`
 * https://reactjs.org/blog/2017/04/07/react-v15.5.0.html#new-deprecation-warnings
 *
 * `import * as React from 'react'` is required by Flowtype https://flow.org/en/docs/react/types/ ,
 * but the * causes both those deprecated getters to be called.
 * This is particularly annoying in Jest since every test prints two useless warnings.
 *
 * This file can be used as a Jest setup file to simply delete those features of the `react` package.
 * You don't need the deprecation warning. Your tests will simply fail if you're still using the old ways.
 * https://facebook.github.io/jest/docs/en/configuration.html#setupfiles-array
 */

delete React.createClass
delete React.PropTypes
