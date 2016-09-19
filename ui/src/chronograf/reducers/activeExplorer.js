export default function activeExplorer(state = {}, action) {
  switch (action.type) {
    case 'LOAD_EXPLORER': {
      return {id: action.payload.explorer.id};
    }
  }

  return state;
}
