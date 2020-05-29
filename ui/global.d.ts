import {MonacoType} from 'src/types'

//
// got some globals here that only exist during compilation
//

declare let monaco: MonacoType

declare global {
  interface Window {
    monaco: MonacoType
  }
}

declare module "*.png" {
   const value: any;
   export = value;
}

window.monaco = window.monaco || {}
