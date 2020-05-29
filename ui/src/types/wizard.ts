import {StepStatus} from 'src/clockface/constants/wizard'
import {Source} from 'src/types'

export interface Step {
  title: string
  stepStatus: StepStatus
}

export type ToggleWizard = (
  isVisible: boolean,
  source?: Source,
  jumpStep?: number,
  showNewKapacitor?: boolean
) => () => void

export interface NextReturn {
  error: boolean
  payload: any
}
