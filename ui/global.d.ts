import {MonacoType} from 'src/types'

//
// got some globals here that only exist during compilation
//

declare module '*.png'
declare let monaco: MonacoType

declare global {
  interface Window {
    monaco: MonacoType
  }
}

window.monaco = window.monaco || {}
