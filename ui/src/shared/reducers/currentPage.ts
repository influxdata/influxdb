export type CurrentPage = 'dashboard' | 'not set'

export type Action = ReturnType<typeof setCurrentPage>

export const setCurrentPage = (currentPage: CurrentPage) =>
  ({type: 'SET_CURRENT_PAGE', currentPage} as const)

// This only exists until we have app-wide color themes.
const currentPage = (state: CurrentPage = 'not set', action: Action) => {
  switch (action.type) {
    case 'SET_CURRENT_PAGE': {
      return action.currentPage
    }

    default:
      return state
  }
}

export default currentPage
