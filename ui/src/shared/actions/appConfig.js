export const setAutoRefresh = (milliseconds) => ({
  type: 'SET_AUTOREFRESH',
  payload: {
    milliseconds,
  },
})
