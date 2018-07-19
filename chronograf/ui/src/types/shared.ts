import {ReactNode} from 'react'

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
  new (
    container: HTMLElement | string,
    data: dygraphs.Data | (() => dygraphs.Data),
    options?: dygraphs.Options
  ): T
}
