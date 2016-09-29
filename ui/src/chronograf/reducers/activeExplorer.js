export default function activeExplorer(state = {}, action) {
  switch (action.type) {
    case 'LOAD_EXPLORER': {
      const {link, name} = action.payload.explorer;
      return {id: link.href, name};
    }
  }

  return state;
}
