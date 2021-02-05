import {MonacoType} from 'src/types'

//
// got some globals here that only exist during compilation
//

declare let monaco: MonacoType

declare global {
  interface Window {
    monaco: MonacoType
  }
  declare module '*.png' {
    const value: any
    export = value
  }

  declare module '*.md' {
    const value: string
    export default value
  }

  declare module '*.svg' {
    export const ReactComponent: SFC<SVGProps<SVGSVGElement>>
    const src: string
    export default src
  }
}

window.monaco = window.monaco || {}
