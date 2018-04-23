import {ReactNode} from 'react'

export type DropdownItem =
  | {
      text: string
    }
  | string

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
