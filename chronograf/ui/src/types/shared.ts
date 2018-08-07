import {ReactNode} from 'react'
import {DygraphData, Options} from 'src/types/dygraphs'

export interface DropdownItem {
  text: string
}

export interface DropdownAction {
  icon: string
  text: string
  handler: () => void
}

export interface PageSection {
  url: string
  name: string
  component: ReactNode
  enabled: boolean
}

export interface Constructable<T> {
  new (container: HTMLElement | string, data: DygraphData, options?: Options): T
}
