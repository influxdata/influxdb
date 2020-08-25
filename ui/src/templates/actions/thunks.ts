// APIs
import {fetchStacks} from 'src/templates/api'

// Actions
import {
  setStacks,
  Action as TemplateAction,
} from 'src/templates/actions/creators'

// Types
import {Dispatch} from 'react'

type Action = TemplateAction

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
