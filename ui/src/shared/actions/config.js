// TODO implement both fns below

export function setAutoRefresh(seconds) {
  // write to local storage
    // on callback & confirmation, return state
  return {
    type: 'SET_AUTOREFRESH',
    payload: {
      seconds,
    },
  };
}

// getAutoRefresh (used by app when loads)
  // load from localstorage
  // return state
