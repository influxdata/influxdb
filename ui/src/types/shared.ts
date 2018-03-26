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
