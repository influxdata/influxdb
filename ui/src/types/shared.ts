import {ReactNode, ComponentType} from 'react'

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

export interface FluxToolbarArg {
  name: string
  desc: string
  type: string
}

export interface FluxToolbarFunction {
  name: string
  args: FluxToolbarArg[]
  desc: string
  package: string
  example: string
  category: string
  link: string
}

export type ErrorMessageComponent = ComponentType<{error: Error}>
