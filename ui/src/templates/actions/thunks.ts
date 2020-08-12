// APIs
import {fetchStacks} from 'src/templates/api'

// Actions
import {Action as NotifyAction} from 'src/shared/actions/notifications'
import {
  setStacks,
  Action as TemplateAction,
} from 'src/templates/actions/creators'

// Types
import {Dispatch} from 'react'

type Action = TemplateAction | NotifyAction

export const fetchAndSetStacks = (orgID: string) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    const stacks = await fetchStacks(orgID)
    dispatch(setStacks(stacks))
  } catch (error) {
    console.error(error)
  }
}
